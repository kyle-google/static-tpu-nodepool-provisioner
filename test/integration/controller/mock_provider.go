package controllertest

import (
	"context"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/cloud"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var _ cloud.Provider = &mockProvider{}

type mockProvider struct {
	sync.Mutex
	gke                    *cloud.GKE
	deleted                map[string]time.Time
	staticNodepoolsCreated map[string]cloud.NodePoolRef
	configsCreated         map[string]*cloud.StaticNodePoolConfig
	ensureCalls            int
	deleteCalls            int
}

func newMockProvider(gke *cloud.GKE) *mockProvider {
	return &mockProvider{
		gke:                    gke,
		deleted:                make(map[string]time.Time),
		staticNodepoolsCreated: make(map[string]cloud.NodePoolRef),
		configsCreated:         make(map[string]*cloud.StaticNodePoolConfig),
	}
}

func (p *mockProvider) ResetCounters() {
	p.Lock()
	defer p.Unlock()
	p.ensureCalls = 0
	p.deleteCalls = 0
}

func (p *mockProvider) EnsureCalls() int {
	p.Lock()
	defer p.Unlock()
	return p.ensureCalls
}

func (p *mockProvider) DeleteCalls() int {
	p.Lock()
	defer p.Unlock()
	return p.deleteCalls
}

func (p *mockProvider) ProjectID() string { return "test-project" }

func (p *mockProvider) DiffStaticNodePools(existingNodepools []cloud.NodePoolRef, desiredNodepools []*cloud.DesiredStaticNodePool) ([]*cloud.DesiredStaticNodePool, []string, []string, []string, error) {
	return p.gke.DiffStaticNodePools(existingNodepools, desiredNodepools)
}

func (p *mockProvider) EnsureStaticNodePools(ctx context.Context, desiredNodePools []*cloud.DesiredStaticNodePool, concurrency int, _ client.Object) error {
	p.Lock()
	p.ensureCalls++
	p.Unlock()

	for _, desired := range desiredNodePools {
		np, err := p.gke.StaticNodePoolForSubBlock(desired.Name, desired.SubblockToConsume, desired.Config)
		if err != nil {
			return err
		}

		p.Lock()
		var subblockName string
		if len(np.Config.ReservationAffinity.Values) > 0 {
			subblockName = np.Config.ReservationAffinity.Values[0]
		}
		p.staticNodepoolsCreated[desired.Name] = cloud.NodePoolRef{
			Name:         desired.Name,
			Labels:       np.Config.Labels,
			SubblockName: subblockName,
		}
		p.configsCreated[desired.Name] = desired.Config
		p.Unlock()
	}
	return nil
}

func (p *mockProvider) DeleteStaticNodePools(ctx context.Context, nodepoolNames []string, concurrency int, eventObj client.Object, why string) []error {
	p.Lock()
	p.deleteCalls++
	p.Unlock()

	for _, name := range nodepoolNames {
		p.DeleteNodePool(name, eventObj, why)
	}
	return nil
}

func (p *mockProvider) ListNodePools() ([]cloud.NodePoolRef, error) {
	p.Lock()
	defer p.Unlock()
	var refs []cloud.NodePoolRef
	for _, ref := range p.staticNodepoolsCreated {
		refs = append(refs, ref)
	}
	return refs, nil
}

func (p *mockProvider) DeleteNodePool(name string, eventObj client.Object, why string) error {
	p.Lock()
	defer p.Unlock()
	if _, exists := p.deleted[name]; !exists {
		p.deleted[name] = time.Now()
	}
	delete(p.staticNodepoolsCreated, name)
	delete(p.configsCreated, name)
	return nil
}

func (p *mockProvider) getDeleted(name string) (time.Time, bool) {
	p.Lock()
	defer p.Unlock()
	timestamp, exists := p.deleted[name]
	return timestamp, exists
}

func (p *mockProvider) getConfig(name string) (*cloud.StaticNodePoolConfig, bool) {
	p.Lock()
	defer p.Unlock()
	cfg, exists := p.configsCreated[name]
	return cfg, exists
}
