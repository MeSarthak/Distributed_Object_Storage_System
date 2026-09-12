package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-storage/pkg/config"
	"distributed-storage/pkg/node"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("==========================================================")
	log.Println(" Distributed Object Storage System - Storage Node Service ")
	log.Println("==========================================================")

	// 1. Load Configuration
	cfg, err := config.LoadStorageNodeConfig()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load storage node configuration: %v", err)
	}
	log.Printf("[CONFIG] Node ID: %s | Hostname: %s | Port: %d", cfg.NodeID, cfg.Hostname, cfg.Port)
	log.Printf("[CONFIG] Storage Directory: %s | Simulated Capacity: %d bytes", cfg.StoragePath, cfg.CapacityBytes)

	// 2. Initialize local storage directory
	if err := os.MkdirAll(cfg.StoragePath, 0755); err != nil {
		log.Fatalf("[FATAL] Failed to create storage path %s: %v", cfg.StoragePath, err)
	}

	// 3. Initialize HTTP Server
	router := gin.Default()

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Message: "Storage node is healthy",
			Data: gin.H{
				"node_id":   cfg.NodeID,
				"hostname":  cfg.Hostname,
				"status":    "HEALTHY",
				"timestamp": time.Now().UTC(),
			},
		})
	})

	router.GET("/internal/storage/info", func(c *gin.Context) {
		// Calculate used storage in directory
		var usedBytes int64
		entries, err := os.ReadDir(cfg.StoragePath)
		if err == nil {
			for _, entry := range entries {
				if info, err := entry.Info(); err == nil {
					usedBytes += info.Size()
				}
			}
		}

		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Message: "Node telemetry info retrieved",
			Data: gin.H{
				"node_id":       cfg.NodeID,
				"hostname":      cfg.Hostname,
				"total_storage": cfg.CapacityBytes,
				"used_storage":  usedBytes,
				"status":        types.NodeStatusOnline,
			},
		})
	})

	// -------------------------------------------------------------------------
	// Phase 2.6: Internal storage APIs (used by self-healing engine for replica copy)
	// -------------------------------------------------------------------------
	storageHandler := node.NewStorageHandler(cfg.StoragePath)
	storageHandler.RegisterRoutes(router)

	srv := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("[HTTP] Storage node [%s] listening on port %d", cfg.Hostname, cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Storage node server failed: %v", err)
		}
	}()

	// -------------------------------------------------------------------------
	// Phase 2.5: Heartbeat sender — runs in its own goroutine
	// -------------------------------------------------------------------------
	bgCtx, bgCancel := context.WithCancel(context.Background())
	heartbeatSender := node.NewHeartbeatSender(cfg)
	go heartbeatSender.Run(bgCtx)

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Printf("[SHUTDOWN] Shutting down storage node [%s]...", cfg.Hostname)

	bgCancel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[FATAL] Storage node forced to shutdown: %v", err)
	}

	log.Printf("[SHUTDOWN] Storage node [%s] exited cleanly.", cfg.Hostname)
}
