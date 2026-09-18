package coordinator

import (
	"fmt"
	"strings"

	"distributed-storage/pkg/types"
)

// BuildNodeURL constructs the internal HTTP base URL for a storage node.
// It gracefully handles container service names, host:port specifications,
// and IP overrides so inter-service communication succeeds in both
// Docker Compose and local development topologies.
func BuildNodeURL(node types.StorageNode) string {
	if strings.HasPrefix(node.Hostname, "http://") || strings.HasPrefix(node.Hostname, "https://") {
		return strings.TrimRight(node.Hostname, "/")
	}
	if strings.Contains(node.Hostname, ":") {
		return fmt.Sprintf("http://%s", node.Hostname)
	}

	if node.IPAddress != "" && node.IPAddress != "0.0.0.0" {
		if strings.Contains(node.IPAddress, ":") {
			return fmt.Sprintf("http://%s", node.IPAddress)
		}
		return fmt.Sprintf("http://%s:9001", node.IPAddress)
	}

	return fmt.Sprintf("http://%s:9001", node.Hostname)
}
