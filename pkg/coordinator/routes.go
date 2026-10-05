package coordinator

import (
	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
)

// APIDependencies bundles the database repositories and services required by
// all Phase 3 API handlers.
type APIDependencies struct {
	DB              *database.DB
	UserRepo        *database.UserRepository
	NodeRepo        *database.NodeRepository
	ReplicaRepo     *database.ReplicaRepository
	ObjectRepo      *database.ObjectRepository
	LogRepo         *database.LogRepository
	AccessLogRepo   *database.AccessLogRepository
	PlacementEngine *PlacementEngine
	AuthService     *auth.AuthService
}

// RegisterAPIRoutes mounts all Phase 3 endpoints onto the Gin router with appropriate
// middleware protections (JWT authentication and Role-Based Access Control).
func RegisterAPIRoutes(router *gin.Engine, deps *APIDependencies) {
	authMiddleware := auth.AuthMiddleware(deps.AuthService)
	adminMiddleware := auth.RequireRole(types.RoleAdmin)

	// Initialize modular handlers
	uploadHandler := NewObjectUploadHandler(
		deps.DB, deps.ObjectRepo, deps.ReplicaRepo,
		deps.NodeRepo, deps.PlacementEngine, deps.LogRepo,
	)

	downloadHandler := NewObjectDownloadHandler(
		deps.DB, deps.ObjectRepo, deps.ReplicaRepo,
		deps.NodeRepo, deps.AccessLogRepo, deps.LogRepo,
	)

	deleteHandler := NewObjectDeleteHandler(
		deps.DB, deps.ObjectRepo, deps.ReplicaRepo,
		deps.NodeRepo, deps.LogRepo,
	)

	monitoringHandler := NewMonitoringHandler(
		deps.DB, deps.NodeRepo, deps.ObjectRepo,
		deps.ReplicaRepo, deps.LogRepo, deps.AccessLogRepo,
	)

	// -------------------------------------------------------------------------
	// 3.2 Object APIs (Protected with JWT)
	// -------------------------------------------------------------------------
	objects := router.Group("/api/objects", authMiddleware)
	{
		objects.POST("", uploadHandler.Upload)
		objects.GET("/:id", downloadHandler.Download)
		objects.DELETE("/:id", deleteHandler.Delete)
		objects.GET("", downloadHandler.List)
		objects.GET("/search", downloadHandler.Search)
	}

	// -------------------------------------------------------------------------
	// 3.3 Metadata APIs (Protected with JWT)
	// -------------------------------------------------------------------------
	router.GET("/api/metadata/:id", authMiddleware, monitoringHandler.Metadata)

	// -------------------------------------------------------------------------
	// 3.5 Monitoring & Admin APIs (Protected with JWT + Role ADMIN)
	// -------------------------------------------------------------------------
	cluster := router.Group("/api/cluster", authMiddleware)
	{
		// Cluster status is visible to all authenticated users
		cluster.GET("/status", monitoringHandler.ClusterStatus)
		// Detailed hardware node metrics require ADMIN role
		cluster.GET("/nodes", adminMiddleware, monitoringHandler.ClusterNodes)
	}

	// Audit logs require ADMIN role
	router.GET("/api/logs", authMiddleware, adminMiddleware, monitoringHandler.SystemLogs)
}
