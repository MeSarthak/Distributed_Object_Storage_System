package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// NodeReader abstracts querying online storage nodes.
type NodeReader interface {
	GetOnlineNodes() ([]types.StorageNode, error)
}

// PlacementEngine selects the optimal storage nodes for a new replica using a
// multi-metric weighted scoring formula or other configured placement strategies.
// All weights and exclusion thresholds are sourced from PlacementConfig — nothing is hardcoded.
type PlacementEngine struct {
	nodeReader NodeReader
	cfg        config.PlacementConfig
	rrIndex    int
	mu         sync.Mutex
}

// NewPlacementEngine constructs a PlacementEngine.
func NewPlacementEngine(nodeReader NodeReader, cfg config.PlacementConfig) *PlacementEngine {
	if cfg.Strategy == "" {
		cfg.Strategy = string(types.PlacementStrategyWeighted)
	}
	return &PlacementEngine{
		nodeReader: nodeReader,
		cfg:        cfg,
	}
}

// candidateNode pairs a StorageNode with its computed placement score.
type candidateNode struct {
	node  types.StorageNode
	score float64
}

// SelectNodes returns up to `count` ONLINE storage nodes that are eligible for
// replica placement. Nodes whose IDs appear in `excludeNodeIDs` are skipped
// (e.g. the failed node and any nodes already holding a replica of the object).
//
// When strategy is "weighted" (default), the scoring formula for each eligible node is:
//
//	score = W_storage  * storageFreeScore
//	      + W_cpu      * (1 - cpuUsage/100)
//	      + W_ram      * (1 - memoryUsage/100)
//	      + W_latency  * latencyScore
//	      + W_health   * 1.0          (node is ONLINE by this point)
//
// When strategy is "least_loaded", nodes are ranked by lowest combined (CPU + RAM).
// When strategy is "round_robin", nodes are picked sequentially across invocations.
//
// Returns an error if fewer than `count` eligible nodes exist.
func (pe *PlacementEngine) SelectNodes(
	ctx context.Context,
	count int,
	excludeNodeIDs []uuid.UUID,
) ([]types.StorageNode, error) {
	if count <= 0 {
		return nil, fmt.Errorf("placement: count must be positive (got %d)", count)
	}

	onlineNodes, err := pe.nodeReader.GetOnlineNodes()
	if err != nil {
		return nil, fmt.Errorf("placement: fetch online nodes: %w", err)
	}

	// Build exclusion set for O(1) lookup.
	excluded := make(map[uuid.UUID]struct{}, len(excludeNodeIDs))
	for _, id := range excludeNodeIDs {
		excluded[id] = struct{}{}
	}

	var candidates []candidateNode
	for _, node := range onlineNodes {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Honour explicit exclusions.
		if _, skip := excluded[node.NodeID]; skip {
			continue
		}

		// Honour hard exclusion thresholds.
		if node.CPUUsage > pe.cfg.MaxCPUThreshold {
			continue
		}
		if node.MemoryUsage > pe.cfg.MaxRAMThreshold {
			continue
		}
		freeBytes := node.TotalStorage - node.UsedStorage
		if freeBytes < pe.cfg.MinFreeStorageBytes {
			continue
		}

		score := pe.scoreNode(node)
		candidates = append(candidates, candidateNode{node: node, score: score})
	}

	if len(candidates) < count {
		return nil, fmt.Errorf(
			"placement: need %d nodes but only %d eligible (online=%d, excluded=%d)",
			count, len(candidates), len(onlineNodes), len(excludeNodeIDs),
		)
	}

	switch types.PlacementStrategy(pe.cfg.Strategy) {
	case types.PlacementStrategyLeastLoaded:
		// Sort ascending by combined CPU and RAM utilization. Tie-break by free storage descending.
		sort.Slice(candidates, func(i, j int) bool {
			loadI := candidates[i].node.CPUUsage + candidates[i].node.MemoryUsage
			loadJ := candidates[j].node.CPUUsage + candidates[j].node.MemoryUsage
			if math.Abs(loadI-loadJ) > 0.001 {
				return loadI < loadJ
			}
			freeI := candidates[i].node.TotalStorage - candidates[i].node.UsedStorage
			freeJ := candidates[j].node.TotalStorage - candidates[j].node.UsedStorage
			return freeI > freeJ
		})

		selected := make([]types.StorageNode, count)
		for i := 0; i < count; i++ {
			selected[i] = candidates[i].node
		}
		return selected, nil

	case types.PlacementStrategyRoundRobin:
		// Sort deterministically by hostname so round-robin ordering is stable across runs.
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].node.Hostname < candidates[j].node.Hostname
		})

		pe.mu.Lock()
		defer pe.mu.Unlock()

		selected := make([]types.StorageNode, count)
		for i := 0; i < count; i++ {
			idx := (pe.rrIndex + i) % len(candidates)
			selected[i] = candidates[idx].node
		}
		pe.rrIndex = (pe.rrIndex + count) % len(candidates)
		return selected, nil

	default:
		// Default: Weighted scoring. Sort descending by score.
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].score > candidates[j].score
		})

		selected := make([]types.StorageNode, count)
		for i := 0; i < count; i++ {
			selected[i] = candidates[i].node
		}
		return selected, nil
	}
}

// scoreNode computes the weighted placement score for a single node.
// All component scores are normalised to [0, 1] so that the weights
// (which must sum to 1.0 by config validation) produce a total in [0, 1].
func (pe *PlacementEngine) scoreNode(node types.StorageNode) float64 {
	// Storage score: fraction of total capacity that is free.
	var storageFreeScore float64
	if node.TotalStorage > 0 {
		free := float64(node.TotalStorage - node.UsedStorage)
		storageFreeScore = math.Max(0, math.Min(1, free/float64(node.TotalStorage)))
	}

	// CPU score: inverse of utilisation.
	cpuScore := 1.0 - math.Min(1.0, node.CPUUsage/100.0)

	// RAM score: inverse of utilisation.
	ramScore := 1.0 - math.Min(1.0, node.MemoryUsage/100.0)

	// Latency score: sigmoid-like decay; 0 ms → 1.0, very high ms → ~0.
	// Formula: 1 / (1 + latency_ms/100)
	latencyScore := 1.0 / (1.0 + node.Latency/100.0)

	// Health score: always 1.0 for ONLINE nodes (already filtered).
	healthScore := 1.0

	return pe.cfg.WeightStorage*storageFreeScore +
		pe.cfg.WeightCPU*cpuScore +
		pe.cfg.WeightRAM*ramScore +
		pe.cfg.WeightLatency*latencyScore +
		pe.cfg.WeightHealth*healthScore
}
