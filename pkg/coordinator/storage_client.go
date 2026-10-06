package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"time"

	"distributed-storage/pkg/types"

	"github.com/google/uuid"
)

// StorageClientInterface defines operations for communicating with storage nodes.
type StorageClientInterface interface {
	StoreChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID, fileData []byte) error
	FetchChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (io.ReadCloser, int64, error)
	DeleteChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) error
	VerifyChunk(ctx context.Context, node types.StorageNode, objectID uuid.UUID) (*types.VerifyResult, error)
}

// StorageClient provides standardized HTTP client methods to interact with
// individual storage node internal APIs (/internal/storage/*).
type StorageClient struct {
	httpClient *http.Client
}


// NewStorageClient creates a StorageClient with a default 30-second timeout.
func NewStorageClient() *StorageClient {
	return &StorageClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// StoreChunk uploads raw file bytes to a target storage node's /internal/storage/store.
func (c *StorageClient) StoreChunk(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
	fileData []byte,
) error {
	baseURL := BuildNodeURL(node)
	endpoint := fmt.Sprintf("%s/internal/storage/store", baseURL)

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("object_id", objectID.String()); err != nil {
		return fmt.Errorf("write object_id field: %w", err)
	}

	part, err := writer.CreateFormFile("file", objectID.String())
	if err != nil {
		return fmt.Errorf("create form file part: %w", err)
	}

	if _, err := io.Copy(part, bytes.NewReader(fileData)); err != nil {
		return fmt.Errorf("copy data to form part: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close multipart writer: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &body)
	if err != nil {
		return fmt.Errorf("build POST request to %s: %w", endpoint, err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("POST %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		respBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST %s returned HTTP %d: %s", endpoint, resp.StatusCode, string(respBytes))
	}

	return nil
}

// FetchChunk streams the object payload from a storage node's GET /internal/storage/:id.
// Caller is responsible for closing the returned ReadCloser.
func (c *StorageClient) FetchChunk(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) (io.ReadCloser, int64, error) {
	baseURL := BuildNodeURL(node)
	endpoint := fmt.Sprintf("%s/internal/storage/%s", baseURL, objectID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build GET request to %s: %w", endpoint, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", endpoint, err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("GET %s returned HTTP %d", endpoint, resp.StatusCode)
	}

	return resp.Body, resp.ContentLength, nil
}

// DeleteChunk requests deletion of an object from a storage node's DELETE /internal/storage/:id.
func (c *StorageClient) DeleteChunk(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) error {
	baseURL := BuildNodeURL(node)
	endpoint := fmt.Sprintf("%s/internal/storage/%s", baseURL, objectID)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build DELETE request to %s: %w", endpoint, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("DELETE %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("DELETE %s returned HTTP %d", endpoint, resp.StatusCode)
	}

	return nil
}

// VerifyChunk asks a storage node to verify the SHA-256 integrity of an object.
func (c *StorageClient) VerifyChunk(
	ctx context.Context,
	node types.StorageNode,
	objectID uuid.UUID,
) (*types.VerifyResult, error) {
	baseURL := BuildNodeURL(node)
	endpoint := fmt.Sprintf("%s/internal/storage/%s/verify", baseURL, objectID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build verify request to %s: %w", endpoint, err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("verify %s returned HTTP %d", endpoint, resp.StatusCode)
	}

	var response types.StandardResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decode verify response from %s: %w", endpoint, err)
	}

	dataBytes, err := json.Marshal(response.Data)
	if err != nil {
		return nil, fmt.Errorf("remarshal verify response: %w", err)
	}

	var result types.VerifyResult
	if err := json.Unmarshal(dataBytes, &result); err != nil {
		return nil, fmt.Errorf("unmarshal verify result: %w", err)
	}

	return &result, nil
}
