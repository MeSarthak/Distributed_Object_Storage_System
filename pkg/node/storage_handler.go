package node

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"distributed-storage/pkg/types"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// StorageHandler provides the internal HTTP handlers that the coordinator
// (and specifically the self-healing engine) uses to store, retrieve, and
// delete object data on this node.
//
// Routes registered:
//
//	POST   /internal/storage/store   — receive and write object data
//	GET    /internal/storage/:id     — stream object data to caller
//	DELETE /internal/storage/:id     — delete object data from local disk
type StorageHandler struct {
	storagePath string
}

// NewStorageHandler constructs a StorageHandler for the given storage directory.
func NewStorageHandler(storagePath string) *StorageHandler {
	return &StorageHandler{storagePath: storagePath}
}

// RegisterRoutes attaches the three internal storage routes to the given Gin engine.
func (sh *StorageHandler) RegisterRoutes(router *gin.Engine) {
	internal := router.Group("/internal/storage")
	{
		internal.POST("/store", sh.Store)
		internal.GET("/:id", sh.Get)
		internal.DELETE("/:id", sh.Delete)
	}
}

// Store handles POST /internal/storage/store.
// Expects a multipart form with:
//
//	object_id — UUID string identifying the object
//	file      — raw file bytes
//
// Writes the file to {storagePath}/{object_id}.
func (sh *StorageHandler) Store(c *gin.Context) {
	objectID := c.PostForm("object_id")
	if objectID == "" {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object_id is required",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}
	// Validate that object_id is a well-formed UUID to prevent path traversal.
	if _, err := uuid.Parse(objectID); err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object_id must be a valid UUID",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	file, _, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "file part is required: " + err.Error(),
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}
	defer file.Close()

	destPath := filepath.Join(sh.storagePath, objectID)

	// Ensure the directory exists (it should already, but defensive).
	if err := os.MkdirAll(sh.storagePath, 0755); err != nil {
		log.Printf("[STORAGE] ERROR creating storage dir %s: %v", sh.storagePath, err)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to create storage directory",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	out, err := os.Create(destPath)
	if err != nil {
		log.Printf("[STORAGE] ERROR creating file %s: %v", destPath, err)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to create object file",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}
	defer out.Close()

	written, err := io.Copy(out, file)
	if err != nil {
		log.Printf("[STORAGE] ERROR writing file %s: %v", destPath, err)
		// Remove partial file.
		_ = os.Remove(destPath)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to write object data",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	log.Printf("[STORAGE] Stored object %s (%d bytes) at %s", objectID, written, destPath)
	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Object stored successfully",
		Data: gin.H{
			"object_id":     objectID,
			"bytes_written": written,
		},
	})
}

// Get handles GET /internal/storage/:id.
// Streams the raw file bytes back to the caller.
// Returns 404 if the object file does not exist on this node.
func (sh *StorageHandler) Get(c *gin.Context) {
	objectID := c.Param("id")
	if objectID == "" {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object id is required",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}
	// Validate UUID to prevent path traversal via crafted :id parameters.
	if _, err := uuid.Parse(objectID); err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object id must be a valid UUID",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	filePath := filepath.Join(sh.storagePath, objectID)

	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, types.StandardResponse{
				Success:   false,
				Message:   "object not found on this node",
				ErrorCode: types.ErrCodeObjectNotFound,
			})
			return
		}
		log.Printf("[STORAGE] ERROR opening file %s: %v", filePath, err)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to open object file",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		log.Printf("[STORAGE] ERROR stat file %s: %v", filePath, err)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to stat object file",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	log.Printf("[STORAGE] Streaming object %s (%d bytes)", objectID, info.Size())
	c.DataFromReader(http.StatusOK, info.Size(), "application/octet-stream", f, nil)
}

// Delete handles DELETE /internal/storage/:id.
// Removes the object file from the local storage directory.
// Returns 404 if the file does not exist (idempotent).
func (sh *StorageHandler) Delete(c *gin.Context) {
	objectID := c.Param("id")
	if objectID == "" {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object id is required",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}
	// Validate UUID to prevent path traversal via crafted :id parameters.
	if _, err := uuid.Parse(objectID); err != nil {
		c.JSON(http.StatusBadRequest, types.StandardResponse{
			Success:   false,
			Message:   "object id must be a valid UUID",
			ErrorCode: types.ErrCodeValidationFailed,
		})
		return
	}

	filePath := filepath.Join(sh.storagePath, objectID)

	err := os.Remove(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, types.StandardResponse{
				Success:   false,
				Message:   "object not found on this node",
				ErrorCode: types.ErrCodeObjectNotFound,
			})
			return
		}
		log.Printf("[STORAGE] ERROR deleting file %s: %v", filePath, err)
		c.JSON(http.StatusInternalServerError, types.StandardResponse{
			Success:   false,
			Message:   "failed to delete object file",
			ErrorCode: types.ErrCodeMetadataFailure,
		})
		return
	}

	log.Printf("[STORAGE] Deleted object %s", objectID)
	c.JSON(http.StatusOK, types.StandardResponse{
		Success: true,
		Message: "Object deleted successfully",
		Data:    gin.H{"object_id": objectID},
	})
}
