package utils

// IsPartitionIDLabel checks if a label key matches the GKE TPU partition ID pattern.
// Pattern: cloud.google.com/gke-tpu-partition-{topology}-id
// We check for the prefix and suffix.
func IsPartitionIDLabel(key string) bool {
	return len(key) > 38 && // "cloud.google.com/gke-tpu-partition-" is 35 chars
		key[:35] == "cloud.google.com/gke-tpu-partition-" &&
		key[len(key)-3:] == "-id"
}
