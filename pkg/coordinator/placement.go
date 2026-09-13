package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// PlacementEngine selects the optimal storage nodes for a new replica using a
// multi-metric weighted scoring formula.  All weights and exclusion thresholds
// are sourced from PlacementConfig — nothing is hardcoded.
type PlacementEngine struct {
	nodeRepo *database.NodeRepository
	cfg      config.PlacementConfig
}

// NewPlacementEngine constructs a PlacementEngine.
func NewPlacementEngine(nodeRepo *database.NodeRepository, cfg config.PlacementConfig) *PlacementEngine {
	return &PlacementEngine{
		nodeRepo: nodeRepo,
		cfg:      cfg,
	}
}

// candidateNode pairs a StorageNode with its computed placement score.
type candidateNode struct {
	node  types.StorageNode
	score float64
}

// PlacementStrategy represents selectable placement algorithms.
type PlacementStrategy string

const (
	StrategyWeightedScore PlacementStrategy = "WEIGHTED_SCORE"
	StrategyLeastLoaded   PlacementStrategy = "LEAST_LOADED"
	StrategyRoundRobin    PlacementStrategy = "ROUND_ROBIN"
)

// NodeEvaluation holds a granular score and eligibility breakdown for a node.
type NodeEvaluation struct {
	Node             types.StorageNode `json:"node"`
	StorageFreeScore float64           `json:"storage_free_score"`
	CPUScore         float64           `json:"cpu_score"`
	RAMScore         float64           `json:"ram_score"`
	LatencyScore     float64           `json:"latency_score"`
	HealthScore      float64           `json:"health_score"`
	TotalScore       float64           `json:"total_score"`
	Eligible         bool              `json:"eligible"`
	ExclusionReason  string            `json:"exclusion_reason,omitempty"`
}

// SelectNodes returns up to `count` ONLINE storage nodes that are eligible for
// replica placement using the default file size 0.
func (pe *PlacementEngine) SelectNodes(
	ctx context.Context,
	count int,
	excludeNodeIDs []uuid.UUID,
) ([]types.StorageNode, error) {
	return pe.SelectNodesWithFileSize(ctx, count, 0, excludeNodeIDs)
}

// SelectNodesWithFileSize selects up to `count` eligible ONLINE storage nodes,
// taking into account the incoming object's fileSize to ensure sufficient capacity.
func (pe *PlacementEngine) SelectNodesWithFileSize(
	ctx context.Context,
	count int,
	fileSize int64,
	excludeNodeIDs []uuid.UUID,
) ([]types.StorageNode, error) {
	onlineNodes, err := pe.nodeRepo.GetOnlineNodes()
	if err != nil {
		return nil, fmt.Errorf("placement: fetch online nodes: %w", err)
	}

	evaluations := pe.EvaluateNodes(onlineNodes, fileSize, excludeNodeIDs)

	var eligibleCandidates []NodeEvaluation
	for _, eval := range evaluations {
		if eval.Eligible {
			eligibleCandidates = append(eligibleCandidates, eval)
		}
	}

	if len(eligibleCandidates) < count {
		return nil, fmt.Errorf(
			"placement: need %d nodes but only %d eligible (online=%d, excluded=%d)",
			count, len(eligibleCandidates), len(onlineNodes), len(excludeNodeIDs),
		)
	}

	// Sort descending by score so the best nodes come first.
	sort.Slice(eligibleCandidates, func(i, j int) bool {
		return eligibleCandidates[i].TotalScore > eligibleCandidates[j].TotalScore
	})

	selected := make([]types.StorageNode, count)
	for i := 0; i < count; i++ {
		selected[i] = eligibleCandidates[i].Node
	}
	return selected, nil
}

// EvaluateNodes evaluates all given nodes and returns detailed scores and eligibility reasons.
func (pe *PlacementEngine) EvaluateNodes(
	nodes []types.StorageNode,
	fileSize int64,
	excludeNodeIDs []uuid.UUID,
) []NodeEvaluation {
	excluded := make(map[uuid.UUID]struct{}, len(excludeNodeIDs))
	for _, id := range excludeNodeIDs {
		excluded[id] = struct{}{}
	}

	evaluations := make([]NodeEvaluation, 0, len(nodes))
	for _, node := range nodes {
		eval := pe.evaluateSingleNode(node)

		if node.Status != types.NodeStatusOnline {
			eval.Eligible = false
			eval.ExclusionReason = fmt.Sprintf("node status is %s", node.Status)
		} else if _, skip := excluded[node.NodeID]; skip {
			eval.Eligible = false
			eval.ExclusionReason = "explicitly excluded (already holds replica or failed)"
		} else if node.CPUUsage > pe.cfg.MaxCPUThreshold {
			eval.Eligible = false
			eval.ExclusionReason = fmt.Sprintf("CPU usage (%.1f%%) exceeds threshold (%.1f%%)", node.CPUUsage, pe.cfg.MaxCPUThreshold)
		} else if node.MemoryUsage > pe.cfg.MaxRAMThreshold {
			eval.Eligible = false
			eval.ExclusionReason = fmt.Sprintf("RAM usage (%.1f%%) exceeds threshold (%.1f%%)", node.MemoryUsage, pe.cfg.MaxRAMThreshold)
		} else {
			freeBytes := node.TotalStorage - node.UsedStorage
			requiredBytes := pe.cfg.MinFreeStorageBytes
			if fileSize > 0 {
				requiredBytes += fileSize
			}
			if freeBytes < requiredBytes {
				eval.Eligible = false
				eval.ExclusionReason = fmt.Sprintf("free storage (%d bytes) is less than required (%d bytes)", freeBytes, requiredBytes)
			} else {
				eval.Eligible = true
			}
		}

		evaluations = append(evaluations, eval)
	}

	return evaluations
}

// ScoreNode computes the weighted placement score for a single node.
func (pe *PlacementEngine) ScoreNode(node types.StorageNode) float64 {
	return pe.scoreNode(node)
}

// scoreNode computes the weighted placement score for a single node (unexported alias for internal callers).
func (pe *PlacementEngine) scoreNode(node types.StorageNode) float64 {
	eval := pe.evaluateSingleNode(node)
	return eval.TotalScore
}

func (pe *PlacementEngine) evaluateSingleNode(node types.StorageNode) NodeEvaluation {
	var storageFreeScore float64
	if node.TotalStorage > 0 {
		free := float64(node.TotalStorage - node.UsedStorage)
		storageFreeScore = math.Max(0, math.Min(1, free/float64(node.TotalStorage)))
	}

	cpuScore := 1.0 - math.Min(1.0, node.CPUUsage/100.0)
	ramScore := 1.0 - math.Min(1.0, node.MemoryUsage/100.0)
	latencyScore := 1.0 / (1.0 + node.Latency/100.0)
	healthScore := 0.0
	if node.Status == types.NodeStatusOnline {
		healthScore = 1.0
	}

	totalScore := pe.cfg.WeightStorage*storageFreeScore +
		pe.cfg.WeightCPU*cpuScore +
		pe.cfg.WeightRAM*ramScore +
		pe.cfg.WeightLatency*latencyScore +
		pe.cfg.WeightHealth*healthScore

	return NodeEvaluation{
		Node:             node,
		StorageFreeScore: storageFreeScore,
		CPUScore:         cpuScore,
		RAMScore:         ramScore,
		LatencyScore:     latencyScore,
		HealthScore:      healthScore,
		TotalScore:       totalScore,
	}
}

