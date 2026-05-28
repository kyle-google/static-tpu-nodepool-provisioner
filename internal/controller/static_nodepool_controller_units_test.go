package controller

import (
	"testing"

	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/copied/api/v1beta1"
	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/cloud"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetInUseNodepools(t *testing.T) {
	tests := []struct {
		name     string
		slices   []v1beta1.Slice
		nodes    []corev1.Node
		expected map[string]bool
	}{
		{
			name:     "No slices or nodes",
			slices:   []v1beta1.Slice{},
			nodes:    []corev1.Node{},
			expected: map[string]bool{},
		},
		{
			name: "Slice matches Node partition ID",
			slices: []v1beta1.Slice{
				{
					Spec: v1beta1.SliceSpec{
						PartitionIds: []string{"uuid-123"},
					},
				},
			},
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							cloud.GKENodePoolNameLabel:                    "pool-1",
							"cloud.google.com/gke-tpu-partition-4x4x4-id": "uuid-123",
						},
					},
				},
			},
			expected: map[string]bool{
				"pool-1": true,
			},
		},
		{
			name: "Slice does not match Node",
			slices: []v1beta1.Slice{
				{
					Spec: v1beta1.SliceSpec{
						PartitionIds: []string{"uuid-999"},
					},
				},
			},
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							cloud.GKENodePoolNameLabel:                    "pool-1",
							"cloud.google.com/gke-tpu-partition-4x4x4-id": "uuid-123",
						},
					},
				},
			},
			expected: map[string]bool{},
		},
		{
			name: "Multiple nodes, one in use",
			slices: []v1beta1.Slice{
				{
					Spec: v1beta1.SliceSpec{
						PartitionIds: []string{"uuid-123"},
					},
				},
			},
			nodes: []corev1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-1",
						Labels: map[string]string{
							cloud.GKENodePoolNameLabel:                    "pool-1",
							"cloud.google.com/gke-tpu-partition-4x4x4-id": "uuid-123",
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "node-2",
						Labels: map[string]string{
							cloud.GKENodePoolNameLabel:                    "pool-2",
							"cloud.google.com/gke-tpu-partition-4x4x4-id": "uuid-456",
						},
					},
				},
			},
			expected: map[string]bool{
				"pool-1": true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetInUseNodepools(tt.slices, tt.nodes)
			if len(got) != len(tt.expected) {
				t.Errorf("GetInUseNodepools() returned %d items, want %d", len(got), len(tt.expected))
			}
			for k, v := range tt.expected {
				if got[k] != v {
					t.Errorf("GetInUseNodepools()[%q] = %v, want %v", k, got[k], v)
				}
			}
		})
	}
}

type dummyProvider struct {
	cloud.Provider
}

func (d dummyProvider) ProjectID() string {
	return "test-project"
}

func TestConstructDesiredNodePools(t *testing.T) {
	r := &StaticNodepoolReconciler{
		Provider: dummyProvider{},
	}

	t.Run("default config fallback", func(t *testing.T) {
		reservations := []reservation{
			{
				Name: "res-1",
				GscSubblocks: []gscSubblock{
					{
						Block:     "block-alpha",
						Subblocks: "0001-0002",
					},
				},
			},
		}

		defaultConfig := &cloud.StaticNodePoolConfig{
			NodepoolPrefix: "alpha",
			MachineType:    "tpu-v4",
			Topology:       "2x2x2",
			NodeCount:      4,
		}

		pools, err := r.constructDesiredNodePools(reservations, defaultConfig)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(pools) != 2 {
			t.Fatalf("expected 2 nodepools, got %d", len(pools))
		}

		for _, p := range pools {
			if p.Config.MachineType != "tpu-v4" || p.Config.Topology != "2x2x2" || p.Config.NodeCount != 4 {
				t.Errorf("nodepool %s config not matches default config: %+v", p.Name, p.Config)
			}
			if p.Config.NodepoolPrefix != "alpha" {
				t.Errorf("nodepool %s prefix does not match: %q", p.Name, p.Config.NodepoolPrefix)
			}
			if p.Name != "alpha-0001" && p.Name != "alpha-0002" {
				t.Errorf("unexpected pool name: %q", p.Name)
			}
		}
	})

	t.Run("subblock override merges with default config", func(t *testing.T) {
		reservations := []reservation{
			{
				Name: "res-1",
				GscSubblocks: []gscSubblock{
					{
						Block:     "block-alpha",
						Subblocks: "0001-0001",
						NodepoolConfig: &cloud.StaticNodePoolConfig{
							NodepoolPrefix: "alpha",
							Topology:       "4x4x4",
							NodeCount:      16,
						},
					},
				},
			},
		}

		defaultConfig := &cloud.StaticNodePoolConfig{
			NodepoolPrefix: "default-prefix-overridden",
			MachineType:    "tpu-v4",
			Topology:       "2x2x2",
			NodeCount:      4,
		}

		pools, err := r.constructDesiredNodePools(reservations, defaultConfig)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(pools) != 1 {
			t.Fatalf("expected 1 nodepool, got %d", len(pools))
		}

		p := pools[0]
		// Should inherit machineType from default but override topology, nodeCount, and prefix
		if p.Config.MachineType != "tpu-v4" {
			t.Errorf("expected inherited MachineType tpu-v4, got %q", p.Config.MachineType)
		}
		if p.Config.Topology != "4x4x4" {
			t.Errorf("expected overridden Topology 4x4x4, got %q", p.Config.Topology)
		}
		if p.Config.NodeCount != 16 {
			t.Errorf("expected overridden NodeCount 16, got %d", p.Config.NodeCount)
		}
		if p.Config.NodepoolPrefix != "alpha" {
			t.Errorf("expected overridden NodepoolPrefix alpha, got %q", p.Config.NodepoolPrefix)
		}
		if p.Name != "alpha-0001" {
			t.Errorf("expected pool name alpha-0001, got %s", p.Name)
		}
	})

	t.Run("subblock level config only without default config", func(t *testing.T) {
		reservations := []reservation{
			{
				Name: "res-1",
				GscSubblocks: []gscSubblock{
					{
						Block:     "block-alpha",
						Subblocks: "0001-0001",
						NodepoolConfig: &cloud.StaticNodePoolConfig{
							NodepoolPrefix: "alpha",
							MachineType:    "tpu-v5",
							Topology:       "4x4x4",
							NodeCount:      16,
						},
					},
				},
			},
		}

		pools, err := r.constructDesiredNodePools(reservations, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(pools) != 1 {
			t.Fatalf("expected 1 nodepool, got %d", len(pools))
		}

		p := pools[0]
		if p.Config.MachineType != "tpu-v5" || p.Config.Topology != "4x4x4" || p.Config.NodeCount != 16 || p.Config.NodepoolPrefix != "alpha" {
			t.Errorf("nodepool config does not match subblock config: %+v", p.Config)
		}
		if p.Name != "alpha-0001" {
			t.Errorf("expected pool name alpha-0001, got %s", p.Name)
		}
	})

	t.Run("missing configuration error", func(t *testing.T) {
		reservations := []reservation{
			{
				Name: "res-1",
				GscSubblocks: []gscSubblock{
					{
						Block:     "block-alpha",
						Subblocks: "0001-0001",
					},
				},
			},
		}

		_, err := r.constructDesiredNodePools(reservations, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})
}
