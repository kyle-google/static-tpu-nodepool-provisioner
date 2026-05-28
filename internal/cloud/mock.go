package cloud

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ Provider = &Mock{}

// Mock is useful for local development or debugging purposes to understand what
// the controller would do without it doing anything.
type Mock struct{}

func (m *Mock) ProjectID() string { return "test-project" }

func (m *Mock) ListNodePools() ([]NodePoolRef, error) { return nil, nil }

func (m *Mock) EnsureStaticNodePools(ctx context.Context, desiredNodePools []*DesiredStaticNodePool, concurrency int, eventObj client.Object) error {
	return nil
}

func (m *Mock) DeleteStaticNodePools(ctx context.Context, nodepoolNames []string, concurrency int, eventObj client.Object, why string) []error {
	return nil
}

func (m *Mock) DiffStaticNodePools(existingNodepools []NodePoolRef, desiredNodepools []*DesiredStaticNodePool) ([]*DesiredStaticNodePool, []string, []string, []string, error) {
	return nil, nil, nil, nil, nil
}
