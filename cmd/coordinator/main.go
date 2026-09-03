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
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
)

func main() {
	log.Println("==========================================================")
	log.Println(" Distributed Object Storage System - Coordinator Service  ")
	log.Println("==========================================================")

	// 1. Load Configuration
	cfg, err := config.LoadCoordinatorConfig()
	if err != nil {
		log.Fatalf("[FATAL] Failed to load configuration: %v", err)
	}
	log.Printf("[CONFIG] Environment: %s, Port: %d", cfg.Server.Environment, cfg.Server.Port)

	if cfg.Server.Environment == "production" {
		gin.SetMode(gin.ReleaseMode)
	}

	// 2. Connect to Database & Run Migrations
	db, err := database.Connect(cfg.Database)
	if err != nil {
		log.Fatalf("[FATAL] Database connection failed: %v", err)
	}
	defer db.Close()

	if err := db.RunMigrationsUp("migrations/000001_init_schema.up.sql"); err != nil {
		log.Fatalf("[FATAL] Schema migration failed: %v", err)
	}

	if err := db.CheckSchema(); err != nil {
		log.Fatalf("[FATAL] Schema verification failed: %v", err)
	}

	// 3. Initialize HTTP Router
	router := gin.Default()

	// CORS Middleware
	router.Use(func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT, DELETE")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Basic Healthcheck & Telemetry routes
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Message: "Coordinator service is healthy",
			Data: gin.H{
				"service":   "coordinator",
				"status":    "UP",
				"timestamp": time.Now().UTC(),
			},
		})
	})

	router.GET("/api/cluster/status", func(c *gin.Context) {
		// Cluster status summary (will be populated with live node metrics in Phase 2/3)
		var totalNodes, onlineNodes int
		_ = db.QueryRow("SELECT COUNT(*) FROM storage_nodes").Scan(&totalNodes)
		_ = db.QueryRow("SELECT COUNT(*) FROM storage_nodes WHERE status = 'ONLINE'").Scan(&onlineNodes)

		var totalObjects int64
		_ = db.QueryRow("SELECT COUNT(*) FROM objects").Scan(&totalObjects)

		c.JSON(http.StatusOK, types.StandardResponse{
			Success: true,
			Message: "Cluster status retrieved successfully",
			Data: gin.H{
				"cluster_status": "HEALTHY",
				"total_nodes":    totalNodes,
				"online_nodes":   onlineNodes,
				"total_objects":  totalObjects,
				"timestamp":      time.Now().UTC(),
			},
		})
	})

	// 4. Start HTTP Server with Graceful Shutdown
	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	go func() {
		log.Printf("[HTTP] Coordinator listening on %s:%d", cfg.Server.Host, cfg.Server.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[FATAL] Coordinator server failed: %v", err)
		}
	}()

	// Wait for interrupt signal for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[SHUTDOWN] Shutting down coordinator service...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("[FATAL] Coordinator forced to shutdown: %v", err)
	}

	log.Println("[SHUTDOWN] Coordinator service exited cleanly.")
}
