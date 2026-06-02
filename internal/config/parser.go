package config

import (
	"fmt"

	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/cloud"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
)

// ParseConfigMap parses the reservations and defaultNodepoolConfig from the ConfigMap.
func ParseConfigMap(cm *corev1.ConfigMap) ([]Reservation, *cloud.StaticNodePoolConfig, error) {
	reservationsYAML, ok := cm.Data["reservations"]
	if !ok {
		return nil, nil, fmt.Errorf("no 'reservations' key in configmap")
	}

	var reservations []Reservation
	if err := yaml.Unmarshal([]byte(reservationsYAML), &reservations); err != nil {
		return nil, nil, fmt.Errorf("failed to unmarshal reservations: %w", err)
	}

	var defaultConfig *cloud.StaticNodePoolConfig
	defaultNodepoolConfigYAML, ok := cm.Data["defaultNodepoolConfig"]
	if ok {
		var config cloud.StaticNodePoolConfig
		if err := yaml.Unmarshal([]byte(defaultNodepoolConfigYAML), &config); err != nil {
			return nil, nil, fmt.Errorf("failed to unmarshal defaultNodepoolConfig: %w", err)
		}
		defaultConfig = &config
	}

	return reservations, defaultConfig, nil
}

// MergeConfig merges a subblock-level override config with a fallback global config.
func MergeConfig(global, subblock *cloud.StaticNodePoolConfig) *cloud.StaticNodePoolConfig {
	if global == nil && subblock == nil {
		return nil
	}
	if global == nil {
		return subblock
	}
	if subblock == nil {
		return global
	}

	res := *global
	if subblock.NodepoolPrefix != "" {
		res.NodepoolPrefix = subblock.NodepoolPrefix
	}
	if subblock.MachineType != "" {
		res.MachineType = subblock.MachineType
	}
	if subblock.Accelerator != "" {
		res.Accelerator = subblock.Accelerator
	}
	if subblock.Topology != "" {
		res.Topology = subblock.Topology
	}
	if subblock.NodeCount != 0 {
		res.NodeCount = subblock.NodeCount
	}
	if len(subblock.NodeLabels) > 0 {
		if res.NodeLabels == nil {
			res.NodeLabels = make(map[string]string)
		}
		for k, v := range subblock.NodeLabels {
			res.NodeLabels[k] = v
		}
	}
	if subblock.ShieldedIntegrityMonitoring != nil {
		res.ShieldedIntegrityMonitoring = subblock.ShieldedIntegrityMonitoring
	}
	if subblock.ShieldedSecureBoot != nil {
		res.ShieldedSecureBoot = subblock.ShieldedSecureBoot
	}
	if subblock.MaxPodsPerNode != 0 {
		res.MaxPodsPerNode = subblock.MaxPodsPerNode
	}
	if subblock.EnableAutoRepair != nil {
		res.EnableAutoRepair = subblock.EnableAutoRepair
	}
	if subblock.PlacementPolicy != "" {
		res.PlacementPolicy = subblock.PlacementPolicy
	}
	return &res
}
