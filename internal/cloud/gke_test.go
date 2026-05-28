package cloud

import (
	"context"
	"net/http"
	"sort"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	container "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/googleapi"
	"k8s.io/apimachinery/pkg/runtime"
)

func TestEnsureStaticNodePool(t *testing.T) {
	gke, svc := newTestGKE(t)

	gke.ClusterContext.MaxPodsPerNode = 20

	ctx := context.Background()

	// Test np-name nodepool prefix.
	npNameConfig := &StaticNodePoolConfig{
		MachineType: "tpu7x-standard-4t",
		Accelerator: V7xSliceAccelerator,
		Topology:    "4x4x4",
		NodeCount:   16,
	}

	desired1 := []*DesiredStaticNodePool{
		{
			Name:              "np-name-0001",
			SubblockToConsume: "projects/test-project/reservations/res-1/reservationBlocks/np-name-block/reservationSubBlocks/np-name-block-subblock-0001",
			Config:            npNameConfig,
		},
		{
			Name:              "np-name-0002",
			SubblockToConsume: "projects/test-project/reservations/res-1/reservationBlocks/np-name-block/reservationSubBlocks/np-name-block-subblock-0002",
			Config:            npNameConfig,
		},
	}
	// This call should create np-name-0001 and np-name-0002
	if err := gke.EnsureStaticNodePools(ctx, desired1, 1, nil); err != nil {
		t.Fatalf("EnsureStaticNodePools(): %v", err)
	}
	if got := svc.creates["np-name-0001"]; got != 1 {
		t.Errorf("expected 1 create for np-name-0001, got %d", got)
	}
	if got := svc.creates["np-name-0002"]; got != 1 {
		t.Errorf("expected 1 create for np-name-0002, got %d", got)
	}

	if got := len(svc.nodePools); got != 2 { // 2 from npNameConfig
		t.Fatalf("expected 2 node pools, got %d", got)
	}

	np1 := svc.nodePools["np-name-0001"]
	if np1 == nil {
		t.Fatal("nodepool np-name-0001 not found")
	}
	if got, want := np1.Config.Labels[LabelProvisionerNodepoolID], "np-name-0001"; got != want {
		t.Errorf("got label %q, want %q", got, want)
	}
	if got, want := np1.MaxPodsConstraint.MaxPodsPerNode, int64(20); got != want {
		t.Errorf("got MaxPodsPerNode %d, want %d", got, want)
	}

	np2 := svc.nodePools["np-name-0002"]
	if np2 == nil {
		t.Fatal("nodepool np-name-0002 not found")
	}
	if got, want := np2.Config.Labels[LabelProvisionerNodepoolID], "np-name-0002"; got != want {
		t.Errorf("got label %q, want %q", got, want)
	}
	if got, want := np2.MaxPodsConstraint.MaxPodsPerNode, int64(20); got != want {
		t.Errorf("got MaxPodsPerNode %d, want %d", got, want)
	}

	// Test np-prefix nodepool prefix with different block name.
	npPrefixConfig := &StaticNodePoolConfig{
		MachineType:    "tpu7x-standard-4t",
		Accelerator:    V7xSliceAccelerator,
		Topology:       "4x4x4",
		NodeCount:      16,
		MaxPodsPerNode: 25,
	}
	desired2 := []*DesiredStaticNodePool{
		{
			Name:              "np-prefix-0001",
			SubblockToConsume: "projects/test-project/reservations/res-2/reservationBlocks/block-name-ignored/reservationSubBlocks/block-name-ignored-subblock-0001",
			Config:            npPrefixConfig,
		},
		{
			Name:              "np-prefix-0002",
			SubblockToConsume: "projects/test-project/reservations/res-2/reservationBlocks/block-name-ignored/reservationSubBlocks/block-name-ignored-subblock-0002",
			Config:            npPrefixConfig,
		},
	}
	// This call should create np-prefix-0001 and np-prefix-0002
	if err := gke.EnsureStaticNodePools(ctx, desired2, 1, nil); err != nil {
		t.Fatalf("EnsureStaticNodePools(): %v", err)
	}
	if got := svc.creates["np-prefix-0001"]; got != 1 {
		t.Errorf("expected 1 create for np-prefix-0001, got %d", got)
	}
	if got := svc.creates["np-prefix-0002"]; got != 1 {
		t.Errorf("expected 1 create for np-prefix-0002, got %d", got)
	}

	if got := len(svc.nodePools); got != 4 { // 2 from npNameConfig, 2 from this call
		t.Fatalf("expected 4 node pools, got %d", got)
	}

	np3 := svc.nodePools["np-prefix-0001"]
	if np3 == nil {
		t.Fatal("nodepool np-prefix-0001 not found")
	}
	if got, want := np3.Config.Labels[LabelProvisionerNodepoolID], "np-prefix-0001"; got != want {
		t.Errorf("got label %q, want %q", got, want)
	}
	if got, want := np3.MaxPodsConstraint.MaxPodsPerNode, int64(25); got != want {
		t.Errorf("got MaxPodsPerNode %d, want %d", got, want)
	}

	np4 := svc.nodePools["np-prefix-0002"]
	if np4 == nil {
		t.Fatal("nodepool np-prefix-0002 not found")
	}
	if got, want := np4.Config.Labels[LabelProvisionerNodepoolID], "np-prefix-0002"; got != want {
		t.Errorf("got label %q, want %q", got, want)
	}
	if got, want := np4.MaxPodsConstraint.MaxPodsPerNode, int64(25); got != want {
		t.Errorf("got MaxPodsPerNode %d, want %d", got, want)
	}
}

func newTestGKE(t *testing.T) (*GKE, *mockGKEService) {
	t.Helper()
	gkeSvc := &mockGKEService{
		creates:   make(map[string]int),
		deletes:   make(map[string]int),
		nodePools: make(map[string]*container.NodePool),
	}
	clusterCtx := GKEContext{
		ProjectID:              "test-project",
		MaxPodsPerNode:         16,
		ClusterLocation:        "us-east5",
		Cluster:                "test-cluster",
		NodeZone:               "us-east5-a",
		NodeServiceAccount:     "test-sa@test-project.iam.gserviceaccount.com",
		NodeAdditionalNetworks: "",
		NodeSecondaryDisk:      "test-disk",
		NodeTags:               []string{"foo", "bar"},
		PodToNodeLabels:        nil,
		NodeSecureBoot:         true,
		ForceOnDemand:          false,
	}
	rec := &mockEventRecorder{}
	gke := &GKE{
		NodePools:      gkeSvc,
		ClusterContext: clusterCtx,
		Recorder:       rec,
	}
	return gke, gkeSvc
}

type mockEventRecorder struct{}

func (r *mockEventRecorder) Event(object runtime.Object, eventtype, reason, message string) {}
func (r *mockEventRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
}
func (r *mockEventRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
}

type mockGKEService struct {
	creates   map[string]int
	deletes   map[string]int
	nodePools map[string]*container.NodePool
}

func (g *mockGKEService) Get(ctx context.Context, name string) (*container.NodePool, error) {
	np, ok := g.nodePools[name]
	if !ok {
		return nil, &googleapi.Error{
			Code: http.StatusNotFound,
		}
	}
	return np, nil
}

func (g *mockGKEService) List(ctx context.Context) (*container.ListNodePoolsResponse, error) {
	var resp container.ListNodePoolsResponse
	for _, np := range g.nodePools {
		resp.NodePools = append(resp.NodePools, np)
	}
	return &resp, nil
}

func (g *mockGKEService) Create(ctx context.Context, req *container.CreateNodePoolRequest, callbacks OpCallbacks) error {
	_, alreadyExists := g.nodePools[req.NodePool.Name]
	if alreadyExists {
		return &googleapi.Error{
			Code: http.StatusConflict,
		}
	}
	g.nodePools[req.NodePool.Name] = req.NodePool
	g.creates[req.NodePool.Name]++
	return nil
}

func (g *mockGKEService) Delete(ctx context.Context, name string, callbacks OpCallbacks) error {
	_, ok := g.nodePools[name]
	if !ok {
		return &googleapi.Error{
			Code: http.StatusNotFound,
		}
	}
	delete(g.nodePools, name)
	g.deletes[name]++
	return nil
}

func TestNodePoolHash(t *testing.T) {
	cases := []struct {
		name        string
		A           *container.NodePool
		B           *container.NodePool
		expSameHash bool
	}{
		{
			name:        "two empty",
			A:           &container.NodePool{Config: &container.NodeConfig{}},
			B:           &container.NodePool{Config: &container.NodeConfig{}},
			expSameHash: true,
		},
		{
			name: "different machine type",
			A: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-4t",
				},
			},
			B: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-8t",
				},
			},
			expSameHash: false,
		},
		{
			name: "different labels",
			A: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-4t",
					Labels: map[string]string{
						"a": "b",
					},
				},
			},
			B: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-4t",
					Labels: map[string]string{
						"a": "c",
					},
				},
			},
			expSameHash: false,
		},
		{
			name: "different label order for static nodepool",
			A: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "tpu7x-standard-4t",
					Labels: map[string]string{
						LabelTPUProvisionerStaticNodepool: "true",
						"a":                               "b",
						"c":                               "d",
					},
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{},
				},
				PlacementPolicy: &container.PlacementPolicy{},
			},
			B: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "tpu7x-standard-4t",
					Labels: map[string]string{
						LabelTPUProvisionerStaticNodepool: "true",
						"c":                               "d",
						"a":                               "b",
					},
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{},
				},
				PlacementPolicy: &container.PlacementPolicy{},
			},
			expSameHash: true,
		},
		{
			name: "non hashed upgrade settings",
			A: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-4t",
					Labels: map[string]string{
						"a": "b",
						"c": "d",
					},
				},
				UpgradeSettings: &container.UpgradeSettings{
					MaxSurge: 1,
				},
			},
			B: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "ct5p-hightpu-4t",
					Labels: map[string]string{
						"a": "b",
						"c": "d",
					},
				},
				UpgradeSettings: &container.UpgradeSettings{
					MaxSurge: 2,
				},
			},
			expSameHash: true,
		},
		{
			name: "different placement policy for static nodepool",
			A: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "tpu7x-standard-4t",
					Labels: map[string]string{
						LabelTPUProvisionerStaticNodepool: "true",
						"a":                               "b",
						"c":                               "d",
					},
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{},
				},
				PlacementPolicy: &container.PlacementPolicy{
					PolicyName: "policy-a",
				},
			},
			B: &container.NodePool{
				Config: &container.NodeConfig{
					MachineType: "tpu7x-standard-4t",
					Labels: map[string]string{
						LabelTPUProvisionerStaticNodepool: "true",
						"a":                               "b",
						"c":                               "d",
					},
					ShieldedInstanceConfig: &container.ShieldedInstanceConfig{},
				},
				PlacementPolicy: &container.PlacementPolicy{
					PolicyName: "policy-b",
				},
			},
			expSameHash: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			hashA, err := nodePoolHash(c.A)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			hashB, err := nodePoolHash(c.B)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if c.expSameHash {
				if hashA != hashB {
					t.Errorf("Expected same hash, got %s and %s", hashA, hashB)
				}
			} else {
				if hashA == hashB {
					t.Errorf("Expected different hash, got %s", hashA)
				}
			}
		})
	}
}

func TestParseAdditionalNodeNetworks(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expected      []*container.AdditionalNodeNetworkConfig
		expectedError bool
	}{
		{
			name:          "empty string",
			input:         "",
			expected:      nil,
			expectedError: false,
		},
		{
			name:  "single network",
			input: "vpc1:subnet1",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: "subnet1"},
			},
			expectedError: false,
		},
		{
			name:  "multiple networks",
			input: "vpc1:subnet1,vpc2:subnet2",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: "subnet1"},
				{Network: "vpc2", Subnetwork: "subnet2"},
			},
			expectedError: false,
		},
		{
			name:  "with whitespace",
			input: "  vpc1:subnet1,  vpc2:subnet2  ",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: "subnet1"},
				{Network: "vpc2", Subnetwork: "subnet2"},
			},
			expectedError: false,
		},
		{
			name:          "invalid format",
			input:         "vpc1subnet1",
			expected:      nil,
			expectedError: true,
		},
		{
			name:  "missing subnet",
			input: "vpc1:",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: ""},
			},
			expectedError: false,
		},
		{
			name:  "missing vpc",
			input: ":subnet1",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "", Subnetwork: "subnet1"},
			},
			expectedError: false,
		},
		{
			name:          "just a comma",
			input:         ",",
			expected:      nil,
			expectedError: false,
		},
		{
			name:  "trailing comma",
			input: "vpc1:subnet1,",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: "subnet1"},
			},
			expectedError: false,
		},
		{
			name:  "leading comma",
			input: ",vpc1:subnet1",
			expected: []*container.AdditionalNodeNetworkConfig{
				{Network: "vpc1", Subnetwork: "subnet1"},
			},
			expectedError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseAdditionalNodeNetworks(tc.input)
			if (err != nil) != tc.expectedError {
				t.Fatalf("parseAdditionalNodeNetworks() error = %v, wantErr %v", err, tc.expectedError)
			}
			if diff := cmp.Diff(tc.expected, result); diff != "" {
				t.Errorf("parseAdditionalNodeNetworks() returned diff (-want +got):\n%s", diff)
			}
		})
	}
}

func TestParseSubBlocks(t *testing.T) {
	testCases := []struct {
		name          string
		input         string
		expectedStart int
		expectedEnd   int
		expectedError bool
	}{
		{
			name:          "valid range",
			input:         "1-10",
			expectedStart: 1,
			expectedEnd:   10,
			expectedError: false,
		},
		{
			name:          "single subblock",
			input:         "5",
			expectedStart: 5,
			expectedEnd:   5,
			expectedError: false,
		},
		{
			name:          "single subblock with leading zeros",
			input:         "0005",
			expectedStart: 5,
			expectedEnd:   5,
			expectedError: false,
		},
		{
			name:          "invalid range, start > end",
			input:         "10-1",
			expectedError: true,
		},
		{
			name:          "invalid single subblock, not a number",
			input:         "abc",
			expectedError: true,
		},
		{
			name:          "invalid range, not numbers",
			input:         "a-b",
			expectedError: true,
		},
		{
			name:          "empty string",
			input:         "",
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := ParseSubBlocks(tc.input)
			if (err != nil) != tc.expectedError {
				t.Fatalf("ParseSubBlocks() error = %v, wantErr %v", err, tc.expectedError)
			}
			if !tc.expectedError {
				if start != tc.expectedStart {
					t.Errorf("ParseSubBlocks() start = %v, want %v", start, tc.expectedStart)
				}
				if end != tc.expectedEnd {
					t.Errorf("ParseSubBlocks() end = %v, want %v", end, tc.expectedEnd)
				}
			}
		})
	}
}

func TestDiffStaticNodePools(t *testing.T) {
	gke, _ := newTestGKE(t)

	// Helper to create a DesiredStaticNodePool and its expected hash
	createDesired := func(name string, machineType string) (*DesiredStaticNodePool, string) {
		config := &StaticNodePoolConfig{
			MachineType: machineType,
			Accelerator: "tpu-v5p-slice",
			Topology:    "2x2x2",
			NodeCount:   2,
			NodeLabels:  map[string]string{"foo": "bar"},
		}
		desired := &DesiredStaticNodePool{
			Name:              name,
			SubblockToConsume: "projects/test-project/reservations/res-1/reservationBlocks/block-1/reservationSubBlocks/" + name,
			Config:            config,
		}
		// Calculate expected hash
		np, err := gke.StaticNodePoolForSubBlock(name, desired.SubblockToConsume, config)
		if err != nil {
			t.Fatalf("failed to create node pool for test helper: %v", err)
		}
		hash, ok := np.Config.Labels[LabelNodePoolHash]
		if !ok {
			t.Fatalf("hash not found in test helper")
		}
		return desired, hash
	}

	desiredA, hashA := createDesired("pool-a", "ct5p-hightpu-4t")
	desiredB, _ := createDesired("pool-b", "ct5p-hightpu-4t")
	desiredAUpdated, hashAUpdated := createDesired("pool-a", "ct5p-hightpu-8t") // Different machine type

	if hashA == hashAUpdated {
		t.Fatalf("hashes should strictly differ for different machine types")
	}

	tests := []struct {
		name              string
		existing          []NodePoolRef
		desired           []*DesiredStaticNodePool
		wantCreate        []string
		wantDeleteMissing []string
		wantDeleteUpdate  []string
		wantDeleteError   []string
	}{
		{
			name:       "Create New Nodepool",
			existing:   []NodePoolRef{},
			desired:    []*DesiredStaticNodePool{desiredA},
			wantCreate: []string{"pool-a"},
		},
		{
			name: "No Change",
			existing: []NodePoolRef{
				{Name: "pool-a", Labels: map[string]string{LabelNodePoolHash: hashA, LabelTPUProvisionerStaticNodepool: "true"}},
			},
			desired: []*DesiredStaticNodePool{desiredA},
		},
		{
			name: "Delete Missing",
			existing: []NodePoolRef{
				{Name: "pool-a", Labels: map[string]string{LabelNodePoolHash: hashA, LabelTPUProvisionerStaticNodepool: "true"}},
			},
			desired:           []*DesiredStaticNodePool{},
			wantDeleteMissing: []string{"pool-a"},
		},
		{
			name: "Delete Update (Hash Mismatch)",
			existing: []NodePoolRef{
				{Name: "pool-a", Labels: map[string]string{LabelNodePoolHash: hashA, LabelTPUProvisionerStaticNodepool: "true"}},
			},
			desired:          []*DesiredStaticNodePool{desiredAUpdated},
			wantCreate:       []string{"pool-a"},
			wantDeleteUpdate: []string{"pool-a"},
		},
		{
			name: "Legacy Nodepool (No Hash)",
			existing: []NodePoolRef{
				{Name: "pool-a", Labels: map[string]string{LabelTPUProvisionerStaticNodepool: "true"}},
			},
			desired: []*DesiredStaticNodePool{desiredA},
		},
		{
			name: "Non-Static Nodepool (Ignored)",
			existing: []NodePoolRef{
				{Name: "pool-b", Labels: map[string]string{}},
			},
			desired: []*DesiredStaticNodePool{},
		},
		{
			name: "Multiple Actions",
			existing: []NodePoolRef{
				{Name: "pool-a", Labels: map[string]string{LabelNodePoolHash: hashA, LabelTPUProvisionerStaticNodepool: "true"}},       // Unchanged
				{Name: "pool-b", Labels: map[string]string{LabelNodePoolHash: "old-hash", LabelTPUProvisionerStaticNodepool: "true"}},  // Update
				{Name: "pool-c", Labels: map[string]string{LabelNodePoolHash: "some-hash", LabelTPUProvisionerStaticNodepool: "true"}}, // Delete
			},
			desired: []*DesiredStaticNodePool{
				desiredA,
				desiredB, // pool-b exists but with hashB vs old-hash
			},
			wantCreate:        []string{"pool-b"},
			wantDeleteMissing: []string{"pool-c"},
			wantDeleteUpdate:  []string{"pool-b"},
		},
		{
			name: "Delete Error (Retry)",
			existing: []NodePoolRef{
				{
					Name:   "pool-a",
					Labels: map[string]string{LabelNodePoolHash: hashA, LabelTPUProvisionerStaticNodepool: "true"},
					Error:  true,
				},
			},
			desired:         []*DesiredStaticNodePool{desiredA},
			wantCreate:      []string{"pool-a"},
			wantDeleteError: []string{"pool-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			toCreate, toDeleteMissing, toDeleteUpdate, toDeleteError, err := gke.DiffStaticNodePools(tc.existing, tc.desired)
			if err != nil {
				t.Fatalf("DiffStaticNodePools() error = %v", err)
			}

			var gotCreate []string
			for _, np := range toCreate {
				gotCreate = append(gotCreate, np.Name)
			}
			sort.Strings(gotCreate)
			sort.Strings(tc.wantCreate)
			if diff := cmp.Diff(tc.wantCreate, gotCreate, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("toCreate mismatch (-want +got):\n%s", diff)
			}

			sort.Strings(toDeleteMissing)
			sort.Strings(tc.wantDeleteMissing)
			if diff := cmp.Diff(tc.wantDeleteMissing, toDeleteMissing, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("toDeleteMissing mismatch (-want +got):\n%s", diff)
			}

			sort.Strings(toDeleteUpdate)
			sort.Strings(tc.wantDeleteUpdate)
			if diff := cmp.Diff(tc.wantDeleteUpdate, toDeleteUpdate, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("toDeleteUpdate mismatch (-want +got):\n%s", diff)
			}

			sort.Strings(toDeleteError)
			sort.Strings(tc.wantDeleteError)
			if diff := cmp.Diff(tc.wantDeleteError, toDeleteError, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("toDeleteError mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
