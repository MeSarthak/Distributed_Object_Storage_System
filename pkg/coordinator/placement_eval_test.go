package coordinator

import (
	"testing"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

func TestPlacementEvaluateNodes(t *testing.T) {
	pe := &PlacementEngine{cfg: defaultPlacementCfg()}
	nodes := threeNodeCluster()
	// Mark node-1 as offline
	nodes[0].Status = types.NodeStatusOffline
	// Overload node-2 CPU
	nodes[1].CPUUsage = 95.0

	evals := pe.EvaluateNodes(nodes, 0, []uuid.UUID{nodes[2].NodeID})
	if len(evals) != 3 {
		t.Fatalf("expected 3 evaluations, got %d", len(evals))
	}

	// Node 1: offline
	if evals[0].Eligible {
		t.Errorf("expected node 1 to be ineligible due to status")
	}

	// Node 2: CPU overloaded
	if evals[1].Eligible {
		t.Errorf("expected node 2 to be ineligible due to CPU")
	}

	// Node 3: explicitly excluded
	if evals[2].Eligible {
		t.Errorf("expected node 3 to be ineligible due to explicit exclusion")
	}
}

func TestPlacementSelectNodesWithFileSize(t *testing.T) {
	nodes := threeNodeCluster()
	// Node 1 has 9 GB free
	// Node 2 has 5 GB free
	// Node 3 has 8 GB free
	pe := &PlacementEngine{cfg: defaultPlacementCfg()}

	// We test EvaluateNodes directly with 6GB file size
	evals := pe.EvaluateNodes(nodes, 6*1024*1024*1024, nil)
	// Node 2 has only 5GB free, so it cannot hold 6GB
	if evals[1].Eligible {
		t.Errorf("expected node 2 to be ineligible for 6GB file, freeBytes=%d", nodes[1].TotalStorage-nodes[1].UsedStorage)
	}
	// Node 1 (9GB free) and Node 3 (8GB free) are eligible
	if !evals[0].Eligible || !evals[2].Eligible {
		t.Errorf("expected node 1 and 3 to be eligible for 6GB file")
	}
}
