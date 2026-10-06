package coordinator

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"distributed-storage/pkg/auth"
	"distributed-storage/pkg/database"
	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ObjectUploadHandler manages the upload and replica placement of objects.
// Assigned to: Developer 1
type ObjectUploadHandler struct {
	db              *database.DB
	objectRepo      *database.ObjectRepository
	replicaRepo     *database.ReplicaRepository
	nodeRepo        *database.NodeRepository
	placementEngine *PlacementEngine
	logRepo         *database.LogRepository
	storageClient   StorageClientInterface
}

// SetStorageClient replaces the internal storage client (useful for unit testing).
func (h *ObjectUploadHandler) SetStorageClient(sc StorageClientInterface) {
	h.storageClient = sc
}

// NewObjectUploadHandler constructs an ObjectUploadHandler.
func NewObjectUploadHandler(
	db *database.DB,
	objectRepo *database.ObjectRepository,
	replicaRepo *database.ReplicaRepository,
	nodeRepo *database.NodeRepository,
	placementEngine *PlacementEngine,
	logRepo *database.LogRepository,
) *ObjectUploadHandler {
	return &ObjectUploadHandler{
		db:              db,
		objectRepo:      objectRepo,
		replicaRepo:     replicaRepo,
		nodeRepo:        nodeRepo,
		placementEngine: placementEngine,
		logRepo:         logRepo,
		storageClient:   NewStorageClient(),
	}
}



// Upload handles POST /api/objects.
// Flow:
// 1. Authenticate user from gin context.
// 2. Read multipart file payload.
// 3. Compute SHA-256 checksum and file size.
// 4. Select candidate storage nodes via PlacementEngine.
// 5. Concurrently or sequentially store chunks on target storage nodes.
// 6. Record metadata atomically in PostgreSQL (objects + replicas tables).
// 7. Write audit log entry.
func (h *ObjectUploadHandler) Upload(c *gin.Context) {
	// 1. Extract authenticated user
	userIDVal, exists := c.Get(auth.ContextUserID)
	if !exists {
		c.JSON(http.StatusUnauthorized, types.StandardResponse{
			Success:   false,
			Message:   "Authentication required",
			ErrorCode: types.ErrCodeAuthUnauthorized,
		})
		return
	}
	userID := userIDVal.(uuid.UUID)

	// 2. Parse multipart file
	fileHeader, err := c.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "File is required: " + err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	file, err := fileHeader.Open()
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to read file: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}
	defer file.Close()

	// Read file bytes into memory
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to read file buffer: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	// 3. Compute SHA-256 checksum
	hasher := sha256.New()
	hasher.Write(fileBytes)
	checksum := hex.EncodeToString(hasher.Sum(nil))

	// Determine replication factor
	repFactor := 3
	if factorStr := c.PostForm("replication_factor"); factorStr != "" {
		if f, err := strconv.Atoi(factorStr); err == nil && f > 0 {
			repFactor = f
		}
	}

	// 4. Select target storage nodes via Placement Engine
	targetNodes, err := h.placementEngine.SelectNodes(c.Request.Context(), repFactor, nil)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, types.StandardResponse{
			Success:   false,
			Message:   "Insufficient storage nodes available: " + err.Error(),
			ErrorCode: types.ErrCodeNodeUnavailable,
		})
		return
	}

	objectID := uuid.New()
	objectName := fileHeader.Filename
	if customName := c.PostForm("object_name"); customName != "" {
		objectName = customName
	}
	mimeType := fileHeader.Header.Get("Content-Type")
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	// 5. Store chunks across selected nodes
	type nodeUploadResult struct {
		node types.StorageNode
		err  error
	}
	results := make([]nodeUploadResult, len(targetNodes))

	for i, node := range targetNodes {
		err := h.storageClient.StoreChunk(c.Request.Context(), node, objectID, fileBytes)
		results[i] = nodeUploadResult{node: node, err: err}
	}

	// Check successful uploads
	var successfulNodes []types.StorageNode
	for _, res := range results {
		if res.err == nil {
			successfulNodes = append(successfulNodes, res.node)
		} else {
			log.Printf("[UPLOAD] Failed to store on node %s (%s): %v", res.node.Hostname, res.node.NodeID, res.err)
		}
	}

	if len(successfulNodes) == 0 {
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to store replicas on any storage node",
			ErrorCode: types.ErrCodeReplicaFailure,
		})
		return
	}

	// Helper to rollback chunks on storage nodes if metadata transaction fails
	cleanupChunks := func() {
		for _, node := range successfulNodes {
			_ = h.storageClient.DeleteChunk(c.Request.Context(), node, objectID)
		}
	}

	// 6. Persist metadata atomically in PostgreSQL
	tx, err := h.db.Begin()
	if err != nil {
		cleanupChunks()
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Database transaction failed: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}
	defer tx.Rollback()

	newObj := &types.Object{
		ObjectID:          objectID,
		OwnerID:           userID,
		ObjectName:        objectName,
		FileSize:          int64(len(fileBytes)),
		MimeType:          mimeType,
		Checksum:          checksum,
		UploadTime:        time.Now().UTC(),
		LastAccessed:      time.Now().UTC(),
		ReplicationFactor: repFactor,
	}

	if err := h.objectRepo.CreateObjectTx(tx, newObj); err != nil {
		cleanupChunks()
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to save object metadata: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	for _, node := range successfulNodes {
		if _, err := h.replicaRepo.InsertReplicaTx(tx, objectID, node.NodeID); err != nil {
			cleanupChunks()
			c.JSON(http.StatusInternalServerError, types.StandardResponse{
				Success:   false,
				Message:   "Failed to save replica metadata: " + err.Error(),
				ErrorCode: types.ErrCodeMetadataFailure,
			})
			return
		}
	}

	if err := tx.Commit(); err != nil {
		cleanupChunks()
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "Failed to commit metadata transaction: " + err.Error(),
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}


	// 7. Audit log
	if h.logRepo != nil {
		_ = h.logRepo.InsertSystemLog(
			"OBJECT_UPLOAD",
			fmt.Sprintf("Object %s (%s) uploaded with %d replicas", objectName, objectID, len(successfulNodes)),
			types.SeverityInfo,
			map[string]string{
				"object_id": objectID.String(),
				"user_id":   userID.String(),
				"size":      strconv.FormatInt(int64(len(fileBytes)), 10),
				"replicas":  strconv.Itoa(len(successfulNodes)),
			},
		)
	}

	c.JSON(http.StatusCreated, types.StandardResponse{
		Success: true,
		Message: "Object uploaded and replicated successfully",
		Data: gin.H{
			"object_id":          objectID,
			"object_name":        objectName,
			"file_size":          len(fileBytes),
			"checksum":           checksum,
			"replication_factor": len(successfulNodes),
			"replicas_stored":    len(successfulNodes),
		},
	})
}
