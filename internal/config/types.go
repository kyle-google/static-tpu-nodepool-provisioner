package config

import (
	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/cloud"
)

type GscSubblock struct {
	Block          string                      `yaml:"block"`
	Subblocks      string                      `yaml:"subblocks"`
	NodepoolConfig *cloud.StaticNodePoolConfig `yaml:"nodepoolConfig"`
}

type Reservation struct {
	Name         string        `yaml:"name"`
	GscSubblocks []GscSubblock `yaml:"gscSubblocks"`
}
