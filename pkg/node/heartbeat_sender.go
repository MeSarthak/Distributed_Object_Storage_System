package node

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/types"
)

// HeartbeatSender periodically collects local node metrics and POSTs them to
// the coordinator's heartbeat endpoint.
//
// Design principles:
//   - Any individual heartbeat failure is logged as a warning but never panics
//     or terminates the goroutine — storage operations must not be affected.
//   - CPU and memory usage are simulated with fixed values for the prototype
//     because accessing /proc requires root or additional OS instrumentation.
//     The storage-used metric is computed accurately from the filesystem.
type HeartbeatSender struct {
	cfg    *config.StorageNodeConfig
	client *http.Client
}

// NewHeartbeatSender constructs a HeartbeatSender.
func NewHeartbeatSender(cfg *config.StorageNodeConfig) *HeartbeatSender {
	return &HeartbeatSender{
		cfg: cfg,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// Run starts the heartbeat loop.  It blocks until ctx is cancelled and should
// therefore be launched in its own goroutine.
func (hs *HeartbeatSender) Run(ctx context.Context) {
	log.Printf("[HEARTBEAT] Starting — interval=%v, coordinator=%s",
		hs.cfg.HeartbeatInterval, hs.cfg.CoordinatorURL)

	// Send an immediate heartbeat on start-up so the coordinator knows we exist.
	hs.sendHeartbeat(ctx)

	ticker := time.NewTicker(hs.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("[HEARTBEAT] Shutting down sender.")
			return
		case <-ticker.C:
			hs.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat builds the payload and POSTs it to the coordinator.
func (hs *HeartbeatSender) sendHeartbeat(ctx context.Context) {
	usedBytes := hs.computeUsedStorage()

	payload := types.HeartbeatPayload{
		NodeID:       hs.cfg.NodeID,
		Hostname:     hs.cfg.Hostname,
		IPAddress:    hs.cfg.IPAddress,
		Port:         hs.cfg.Port,
		TotalStorage: hs.cfg.CapacityBytes,
		UsedStorage:  usedBytes,
		// CPU and memory are simulated at 10% for the prototype.
		// Replace with actual /proc/stat or runtime.MemStats reads if needed.
		CPUUsage:    10.0,
		MemoryUsage: 10.0,
		LatencyMs:   0.0,
		Timestamp:   time.Now().UTC(),
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[HEARTBEAT] ERROR marshaling payload: %v", err)
		return
	}

	url := hs.cfg.CoordinatorURL + "/internal/heartbeat"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		log.Printf("[HEARTBEAT] ERROR building request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := hs.client.Do(req)
	if err != nil {
		log.Printf("[HEARTBEAT] WARN: failed to reach coordinator at %s: %v", url, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[HEARTBEAT] WARN: coordinator returned HTTP %d", resp.StatusCode)
		return
	}

	log.Printf("[HEARTBEAT] Sent to coordinator — used=%d/%d bytes, cpu=%.1f%%, mem=%.1f%%",
		usedBytes, hs.cfg.CapacityBytes, payload.CPUUsage, payload.MemoryUsage)
}

// computeUsedStorage walks the storage directory and sums up file sizes.
// Errors are ignored (non-fatal); returns 0 if the directory cannot be read.
func (hs *HeartbeatSender) computeUsedStorage() int64 {
	var total int64
	_ = filepath.WalkDir(hs.cfg.StoragePath, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
