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

// SelectNodes returns up to `count` ONLINE storage nodes that are eligible for
// replica placement.  Nodes whose IDs appear in `excludeNodeIDs` are skipped
// (the failed node and any nodes already holding a replica of the object).
//
// The scoring formula for each eligible node is:
//
//	score = W_storage  * storageFreeScore
//	      + W_cpu      * (1 - cpuUsage/100)
//	      + W_ram      * (1 - memoryUsage/100)
//	      + W_latency  * latencyScore
//	      + W_health   * 1.0          (node is ONLINE by this point)
//
// Where:
//   - storageFreeScore = freeBytes / totalBytes   (clamped [0,1])
//   - latencyScore     = 1 / (1 + latency_ms/100) (approaches 0 for high latency)
//
// Returns an error if fewer than `count` eligible nodes exist.
func (pe *PlacementEngine) SelectNodes(
	ctx context.Context,
	count int,
	excludeNodeIDs []uuid.UUID,
) ([]types.StorageNode, error) {
	onlineNodes, err := pe.nodeRepo.GetOnlineNodes()
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

	// Sort descending by score so the best nodes come first.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	selected := make([]types.StorageNode, count)
	for i := 0; i < count; i++ {
		selected[i] = candidates[i].node
	}
	return selected, nil
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
