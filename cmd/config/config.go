/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	// Provider can be "gke" or "mock".
	Provider string `envconfig:"PROVIDER" default:"gke"`

	GCPProjectID          string `envconfig:"GCP_PROJECT_ID"`
	GCPClusterLocation    string `envconfig:"GCP_CLUSTER_LOCATION"`
	GCPZone               string `envconfig:"GCP_ZONE"`
	GCPCluster            string `envconfig:"GCP_CLUSTER"`
	GCPNodeServiceAccount string `envconfig:"GCP_NODE_SERVICE_ACCOUNT"`

	GCPNodeTags               []string `envconfig:"GCP_NODE_TAGS"`
	GCPNodeSecondaryDisk      string   `envconfig:"GCP_NODE_SECONDARY_DISK" default:""`
	GCPNodeSecureBoot         bool     `envconfig:"GCP_NODE_SECURE_BOOT" default:"true"`
	GCPNodeAdditionalNetworks string   `envconfig:"GCP_NODE_ADDITIONAL_NETWORKS" default:""`

	GCPNodeDiskType            string `envconfig:"GCP_NODE_DISK_TYPE"`
	GCPNodeConfidentialStorage bool   `envconfig:"GCP_NODE_CONFIDENTIAL_STORAGE"`
	GCPNodeBootDiskKMSKey      string `envconfig:"GCP_NODE_BOOT_DISK_KMS_KEY"`

	// GKEMaxPodsPerNode sets the max pods per node in provisioned node pools
	GKEMaxPodsPerNode int `envconfig:"GKE_MAX_PODS_PER_NODE" default:"16"`

	StaticNodepoolCreateConcurrency int           `envconfig:"STATIC_NODEPOOL_CREATE_CONCURRENCY" default:"3"`
	StaticNodepoolCreateTimeout     time.Duration `envconfig:"STATIC_NODEPOOL_CREATE_TIMEOUT" default:"10m"`
	StaticNodepoolDeleteConcurrency int           `envconfig:"STATIC_NODEPOOL_DELETE_CONCURRENCY" default:"3"`

	PodNamespace string `envconfig:"POD_NAMESPACE"`
}

func ParseEnv() (Config, error) {
	cfg := Config{}
	err := envconfig.Process("", &cfg)
	return cfg, err
}
