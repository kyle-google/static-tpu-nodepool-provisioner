package cloud

import (
	"context"
	"errors"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	keyPrefix = "google.com/"

	LabelNodepoolManager             = keyPrefix + "nodepool-manager"
	LabelNodepoolManagerTPUPodinator = "tpu-provisioner"

	LabelNodePoolHash = keyPrefix + "tpu-provisioner-nodepool-hash"

	LabelProvisionerNodepoolID        = "provisioner-nodepool-id"
	LabelTPUProvisionerStaticNodepool = "tpu-provisioner-static-nodepool"

	GKENodePoolNameLabel = "cloud.google.com/gke-nodepool"

	EventNodePoolCreationStarted   = "NodePoolCreationStarted"
	EventNodePoolCreationSucceeded = "NodePoolCreationSucceeded"
	EventNodePoolCreationFailed    = "NodePoolCreationFailed"

	EventNodePoolDeletionStarted   = "NodePoolDeletionStarted"
	EventNodePoolDeletionSucceeded = "NodePoolDeletionSucceeded"
	EventNodePoolDeletionFailed    = "NodePoolDeletionFailed"

	EventNodePoolNotFound = "NodePoolNotFound"
)

type StaticNodePoolConfig struct {
	NodepoolPrefix              string            `yaml:"nodepoolPrefix"`
	MachineType                 string            `yaml:"machineType"`
	Accelerator                 string            `yaml:"accelerator"`
	Topology                    string            `yaml:"topology"`
	NodeCount                   int               `yaml:"nodeCount"`
	NodeLabels                  map[string]string `yaml:"nodeLabels"`
	ShieldedIntegrityMonitoring *bool             `yaml:"shieldedIntegrityMonitoring"`
	ShieldedSecureBoot          *bool             `yaml:"shieldedSecureBoot"`
	MaxPodsPerNode              int64             `yaml:"maxPodsPerNode"`
	EnableAutoRepair            *bool             `yaml:"enableAutorepair"`
	PlacementPolicy             string            `yaml:"placementPolicy"`
}

type DesiredStaticNodePool struct {
	Name              string
	ReservationName   string
	GscBlockName      string
	NodepoolPrefix    string
	SubblockIndex     int
	Config            *StaticNodePoolConfig
	SubblockToConsume string
}

type Provider interface {
	ProjectID() string
	ListNodePools() ([]NodePoolRef, error)
	EnsureStaticNodePools(ctx context.Context, desiredNodePools []*DesiredStaticNodePool, concurrency int, eventObj client.Object) error
	DeleteStaticNodePools(ctx context.Context, nodepoolNames []string, concurrency int, eventObj client.Object, why string) []error
	DiffStaticNodePools(existingNodepools []NodePoolRef, desiredNodepools []*DesiredStaticNodePool) (toCreate []*DesiredStaticNodePool, toDeleteMissing []string, toDeleteUpdate []string, toDeleteError []string, err error)
}

var ErrDuplicateRequest = errors.New("duplicate request")

type NodePoolRef struct {
	Name string

	CreationTime time.Time

	Labels       map[string]string
	SubblockName string

	Error   bool
	Message string
}
