package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	containerv1beta1 "google.golang.org/api/container/v1beta1"
	"google.golang.org/api/googleapi"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("provider")

const (
	// Supported accelerator types
	V7xSliceAccelerator = "tpu7x"

	// Supported dynamic labels or constants
	V7xPlacementPolicyPrefix = "tpu-provisioner-"
)

var _ Provider = &GKE{}

type GKE struct {
	NodePools               NodePoolService
	ClusterContext          GKEContext
	Recorder                record.EventRecorder
	inProgressDeletesNPName sync.Map
}

func (g *GKE) ProjectID() string { return g.ClusterContext.ProjectID }

func (g *GKE) ListNodePools() ([]NodePoolRef, error) {
	var refs []NodePoolRef

	resp, err := g.NodePools.List(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("listing node pools: %w", err)
	}

	for _, np := range resp.NodePools {
		var labels map[string]string
		if np.Config != nil {
			labels = np.Config.Labels
		}

		var subblockName string
		if np.Config != nil && np.Config.ReservationAffinity != nil && len(np.Config.ReservationAffinity.Values) > 0 {
			subblockName = np.Config.ReservationAffinity.Values[0]
		}

		refs = append(refs, NodePoolRef{
			Name:         np.Name,
			Error:        np.Status == "ERROR",
			Message:      np.StatusMessage,
			Labels:       labels,
			SubblockName: subblockName,
		})
	}

	return refs, nil
}

func (g *GKE) DeleteNodePool(name string, eventObj client.Object, why string) error {
	// Due to concurrent reconciles, multiple deletes for the same
	// Node Pool will occur at the same time. To avoid a bunch of failed requests, we deduplicate here.
	if _, inProgress := g.inProgressDeletesNPName.Load(name); inProgress {
		return ErrDuplicateRequest
	}
	g.inProgressDeletesNPName.Store(name, struct{}{})
	defer g.inProgressDeletesNPName.Delete(name)

	g.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolDeletionStarted, "Starting deletion of Node Pool %s because %s", name, why)
	if err := g.NodePools.Delete(context.TODO(), name, OpCallbacks{
		NotFound: func() {
			g.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolNotFound, "Node pool not found - ignoring deletion attempt.", name)
		},
		ReqFailure: func(err error) {
			g.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolDeletionFailed, "Request to delete Node Pool %s failed: %v.", name, err)
		},
		OpFailure: func(err error) {
			g.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolDeletionFailed, "Operation to delete Node Pool %s failed: %v.", name, err)
		},
		Success: func() {
			g.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolDeletionSucceeded, "Successfully deleted Node Pool %s.", name)
		},
	}); err != nil {
		return err
	}

	return nil
}

func (g *GKE) DiffStaticNodePools(existingNodepools []NodePoolRef, desiredNodepools []*DesiredStaticNodePool) ([]*DesiredStaticNodePool, []string, []string, []string, error) {
	var toCreate []*DesiredStaticNodePool
	var toDeleteMissing []string
	var toDeleteUpdate []string
	var toDeleteError []string

	existingMap := make(map[string]NodePoolRef)
	for _, np := range existingNodepools {
		existingMap[np.Name] = np
	}

	desiredMap := make(map[string]*DesiredStaticNodePool)
	for _, np := range desiredNodepools {
		desiredMap[np.Name] = np
	}

	// Find nodepools to create or update.
	for _, desired := range desiredNodepools {
		desiredNP, err := g.StaticNodePoolForSubBlock(desired.Name, desired.SubblockToConsume, desired.Config)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("failed to build desired nodepool object for %s: %w", desired.Name, err)
		}
		desiredHash, ok := desiredNP.Config.Labels[LabelNodePoolHash]
		if !ok {
			return nil, nil, nil, nil, fmt.Errorf("missing hash in desired node pool %s", desired.Name)
		}

		existing, ok := existingMap[desired.Name]
		if !ok {
			toCreate = append(toCreate, desired)
			continue
		}

		// If the existing nodepool is in an ERROR state, we should recreate it regardless of whether the config changed or not.
		if existing.Error {
			toDeleteError = append(toDeleteError, desired.Name)
			toCreate = append(toCreate, desired)
			continue
		}

		existingHash, ok := existing.Labels[LabelNodePoolHash]
		// If existing nodepool has no hash, we assume it's a legacy one and don't touch it unless it is removed from config.
		// If it's removed from config, it will be caught in the next loop.
		if ok && existingHash != desiredHash {
			toDeleteUpdate = append(toDeleteUpdate, desired.Name)
			toCreate = append(toCreate, desired)
		}
	}

	// Find nodepools to delete.
	for _, existing := range existingNodepools {
		if existing.Labels[LabelTPUProvisionerStaticNodepool] != "true" {
			continue
		}
		if _, ok := desiredMap[existing.Name]; !ok {
			toDeleteMissing = append(toDeleteMissing, existing.Name)
		}
	}

	return toCreate, toDeleteMissing, toDeleteUpdate, toDeleteError, nil
}

// EnsureStaticNodePools provisions all the node pools for a given reservation.
func (g *GKE) EnsureStaticNodePools(ctx context.Context, desiredNodePools []*DesiredStaticNodePool, concurrency int, eventObj client.Object) error {
	log.Info("Ensuring static nodepools", "count", len(desiredNodePools))

	var wg sync.WaitGroup
	errs := make(chan error, len(desiredNodePools))
	sem := make(chan struct{}, concurrency)

	for _, desired := range desiredNodePools {
		wg.Add(1)
		sem <- struct{}{}

		go func(desired *DesiredStaticNodePool) {
			defer wg.Done()
			defer func() { <-sem }()

			np, err := g.StaticNodePoolForSubBlock(desired.Name, desired.SubblockToConsume, desired.Config)
			if err != nil {
				errs <- fmt.Errorf("determining node pool for static reservation: %w", err)
				return
			}
			log.Info("Determined node pool for static reservation", "nodePoolName", np.Name, "nodePool", np)

			npExists, err := g.checkNodePoolExists(ctx, np)
			if err != nil {
				errs <- fmt.Errorf("checking if node pool exists: %w", err)
				return
			}
			log.Info("Checked whether static node pool already exists",
				"nodePoolName", np.Name, "existingNodePoolState", npExists,
			)

			if !npExists {
				req := &containerv1beta1.CreateNodePoolRequest{
					NodePool: np,
					Parent:   g.ClusterContext.ClusterName(),
				}

				log.Info("statically creating node pool", "nodePoolName", np.Name, "request", req)
				g.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolCreationStarted, "Starting creation of static Node Pool %s", np.Name)
				if err := g.NodePools.Create(ctx, req, OpCallbacks{
					ReqFailure: func(err error) {
						log.Error(err, "request to create static node pool failed", "nodePoolName", np.Name)
						g.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolCreationFailed, "Request to create Node Pool %s failed: %v.", np.Name, err)
					},
					OpFailure: func(err error) {
						log.Error(err, "operation to create static node pool failed", "nodePoolName", np.Name)
						g.Recorder.Eventf(eventObj, corev1.EventTypeWarning, EventNodePoolCreationFailed, "Operation to create Node Pool %s failed: %v.", np.Name, err)
					},
					Success: func() {
						log.Info("successfully created static node pool", "nodePoolName", np.Name)
						g.Recorder.Eventf(eventObj, corev1.EventTypeNormal, EventNodePoolCreationSucceeded, "Successfully created Node Pool %s.", np.Name)
					},
				}); err != nil {
					errs <- err
				}
			}
		}(desired)
	}

	log.Info("Waiting for all goroutines to finish ensuring static nodepools.")
	wg.Wait()
	close(errs)

	var allErrors []error
	for err := range errs {
		allErrors = append(allErrors, err)
	}

	if len(allErrors) > 0 {
		return errors.Join(allErrors...)
	}

	return nil
}

// ParseSubBlocks parses a string of the form "startSubBlockIndex-endSubBlockIndex" or "startSubBlockIndex" into integers.
func ParseSubBlocks(subblocks string) (int, int, error) {
	if !strings.Contains(subblocks, "-") {
		start, err := strconv.Atoi(subblocks)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid subblock: %s. expected an integer", subblocks)
		}
		return start, start, nil
	}

	parts := strings.Split(subblocks, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid subblocks format: %s. expected format is start-end", subblocks)
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start subblock: %s. expected an integer", parts[0])
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end subblock: %s. expected an integer", parts[1])
	}

	if start > end {
		return 0, 0, fmt.Errorf("start subblock %d cannot be greater than end subblock %d", start, end)
	}

	return start, end, nil
}

func (g *GKE) StaticNodePoolForSubBlock(nodePoolID, subblockToConsume string, config *StaticNodePoolConfig) (*containerv1beta1.NodePool, error) {
	labels := map[string]string{
		LabelNodepoolManager:              LabelNodepoolManagerTPUPodinator,
		LabelProvisionerNodepoolID:        nodePoolID,
		LabelTPUProvisionerStaticNodepool: "true",
	}
	for k, v := range config.NodeLabels {
		labels[k] = v
	}

	reservation := &containerv1beta1.ReservationAffinity{
		ConsumeReservationType: "SPECIFIC_RESERVATION",
		Key:                    "compute.googleapis.com/reservation-name",
		Values: []string{
			subblockToConsume,
		},
	}

	var secondaryDisks []*containerv1beta1.SecondaryBootDisk
	if g.ClusterContext.NodeSecondaryDisk != "" {
		secondaryDisks = []*containerv1beta1.SecondaryBootDisk{
			{
				DiskImage: g.ClusterContext.NodeSecondaryDisk,
				Mode:      "CONTAINER_IMAGE_CACHE",
			},
		}
	}

	var networkConfig *containerv1beta1.NodeNetworkConfig
	additionalNodeNetworks, err := parseAdditionalNodeNetworks(g.ClusterContext.NodeAdditionalNetworks)
	if err != nil {
		return nil, err
	}

	if len(additionalNodeNetworks) > 0 {
		networkConfig = &containerv1beta1.NodeNetworkConfig{
			AdditionalNodeNetworkConfigs: additionalNodeNetworks,
		}
	}

	placementPolicy := &containerv1beta1.PlacementPolicy{}
	if config.PlacementPolicy != "" {
		placementPolicy.PolicyName = config.PlacementPolicy
	} else if config.Accelerator == V7xSliceAccelerator {
		placementPolicy.PolicyName = fmt.Sprintf("tpu-provisioner-%v", config.Topology)
	} else {
		placementPolicy.TpuTopology = config.Topology
		placementPolicy.Type = "COMPACT"
	}

	var diskType string
	if g.ClusterContext.NodeDiskType != "" {
		diskType = g.ClusterContext.NodeDiskType
	}

	// Nodepool name must be <= 40 characters.
	name := nodePoolID
	if len(name) > 40 {
		return nil, fmt.Errorf("generated nodepool name %q is longer than 40 characters. Please specify a custom suffix for the nodepool name in the tpu-provisioner-static-nodepools-config configmap that meets the length requirements", name)
	}

	shieldedIntegrityMonitoring := true
	if config.ShieldedIntegrityMonitoring != nil {
		shieldedIntegrityMonitoring = *config.ShieldedIntegrityMonitoring
	}
	shieldedSecureBoot := g.ClusterContext.NodeSecureBoot
	if config.ShieldedSecureBoot != nil {
		shieldedSecureBoot = *config.ShieldedSecureBoot
	}

	autorepair := true
	if config.EnableAutoRepair != nil {
		autorepair = *config.EnableAutoRepair
	}

	maxPods := g.ClusterContext.MaxPodsPerNode
	if config.MaxPodsPerNode != 0 {
		maxPods = int(config.MaxPodsPerNode)
	}

	locations := []string{g.ClusterContext.NodeZone}

	tags := g.ClusterContext.NodeTags

	np := &containerv1beta1.NodePool{
		Name: name,
		Config: &containerv1beta1.NodeConfig{
			ServiceAccount: g.ClusterContext.NodeServiceAccount,
			ShieldedInstanceConfig: &containerv1beta1.ShieldedInstanceConfig{
				EnableIntegrityMonitoring: shieldedIntegrityMonitoring,
				EnableSecureBoot:          shieldedSecureBoot,
			},
			Tags:                      tags,
			SecondaryBootDisks:        secondaryDisks,
			MachineType:               config.MachineType,
			ReservationAffinity:       reservation,
			Labels:                    labels,
			BootDiskKmsKey:            g.ClusterContext.NodeBootDiskKMSKey,
			DiskType:                  diskType,
			EnableConfidentialStorage: g.ClusterContext.NodeConfidentialStorage,
		},
		InitialNodeCount: int64(config.NodeCount),
		Locations:        locations,
		PlacementPolicy:  placementPolicy,
		Management: &containerv1beta1.NodeManagement{
			AutoRepair:  autorepair,
			AutoUpgrade: false,
		},
		UpgradeSettings: &containerv1beta1.UpgradeSettings{
			MaxSurge: 1,
		},
		MaxPodsConstraint: &containerv1beta1.MaxPodsConstraint{MaxPodsPerNode: int64(maxPods)},
		NetworkConfig:     networkConfig,
	}

	hash, err := nodePoolHash(np)
	if err != nil {
		return nil, fmt.Errorf("hashing node pool: %w", err)
	}
	np.Config.Labels[LabelNodePoolHash] = hash
	return np, nil
}

func nodePoolHash(np *containerv1beta1.NodePool) (string, error) {
	h := fnv.New32a()

	// For static nodepools, we hash all the fields from the configmap.
	type staticNodepoolHashData struct {
		PlacementPolicy        *containerv1beta1.PlacementPolicy
		InitialNodeCount       int64
		MaxPodsConstraint      *containerv1beta1.MaxPodsConstraint
		MachineType            string
		ReservationAffinity    *containerv1beta1.ReservationAffinity
		ShieldedInstanceConfig *containerv1beta1.ShieldedInstanceConfig
		Labels                 map[string]string
	}

	hashData := staticNodepoolHashData{
		PlacementPolicy:   np.PlacementPolicy,
		InitialNodeCount:  np.InitialNodeCount,
		MaxPodsConstraint: np.MaxPodsConstraint,
	}
	if np.Config != nil {
		hashData.MachineType = np.Config.MachineType
		hashData.ReservationAffinity = np.Config.ReservationAffinity
		hashData.ShieldedInstanceConfig = np.Config.ShieldedInstanceConfig
		if np.Config.Labels != nil {
			hashData.Labels = make(map[string]string, len(np.Config.Labels))
			for k, v := range np.Config.Labels {
				// The hash label will change on every hash calculation, so we need to exclude it.
				if k == LabelNodePoolHash {
					continue
				}
				hashData.Labels[k] = v
			}
		}
	}

	jsn, err := json.Marshal(hashData)
	if err != nil {
		return "", err
	}
	h.Write(jsn)
	return rand.SafeEncodeString(fmt.Sprint(h.Sum32())), nil
}

// DeleteStaticNodePools deletes a list of nodepools concurrently.
func (g *GKE) DeleteStaticNodePools(ctx context.Context, nodepoolNames []string, concurrency int, eventObj client.Object, why string) []error {
	lg := logf.FromContext(ctx)

	if concurrency <= 0 {
		concurrency = 1
	}
	if len(nodepoolNames) < concurrency {
		concurrency = len(nodepoolNames)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(nodepoolNames))
	sem := make(chan struct{}, concurrency)

	for _, name := range nodepoolNames {
		wg.Add(1)
		sem <- struct{}{}

		go func(npName string) {
			defer wg.Done()
			defer func() { <-sem }()

			lg.Info("Deleting static nodepool", "nodepool", npName)
			if err := g.DeleteNodePool(npName, eventObj, why); err != nil {
				errs <- fmt.Errorf("failed to delete nodepool %s: %w", npName, err)
			}
		}(name)
	}

	lg.Info("Waiting for all goroutines to finish deleting static nodepools.")
	wg.Wait()
	close(errs)

	var allErrors []error
	for err := range errs {
		allErrors = append(allErrors, err)
	}

	return allErrors
}

func (g *GKE) checkNodePoolExists(ctx context.Context, desired *containerv1beta1.NodePool) (bool, error) {
	_, err := g.NodePools.Get(ctx, desired.Name)
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok && gerr.Code == http.StatusNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func waitForGkeOp(ctx context.Context, svc *containerv1beta1.Service, c GKEContext, operation *containerv1beta1.Operation) error {
	operationWaitTimeout := 30 * time.Minute
	operationPollInterval := 5 * time.Second

	for start := time.Now(); time.Since(start) < operationWaitTimeout; time.Sleep(operationPollInterval) {
		if op, err := svc.Projects.Locations.Operations.Get(c.OpName(operation.Name)).Context(ctx).Do(); err == nil {
			if op.Status == "DONE" {
				return nil
			}
		} else {
			return fmt.Errorf("waiting for operation: %w", err)
		}
	}

	return fmt.Errorf("timeout while waiting for operation %s on %s to complete", operation.Name, operation.TargetLink)
}

func parseAdditionalNodeNetworks(additionalNodeNetworksCSV string) ([]*containerv1beta1.AdditionalNodeNetworkConfig, error) {
	var additionalNodeNetworks []*containerv1beta1.AdditionalNodeNetworkConfig
	// additional-node-networks: "vpc1:subnet1, vpc2:subnet2"
	for _, pair := range strings.Split(additionalNodeNetworksCSV, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		netAndSubnet := strings.SplitN(pair, ":", 2)
		if len(netAndSubnet) != 2 {
			return nil, fmt.Errorf("invalid additional network annotation: %v", pair)
		}

		additionalNodeNetworks = append(additionalNodeNetworks, &containerv1beta1.AdditionalNodeNetworkConfig{
			Network:    strings.TrimSpace(netAndSubnet[0]),
			Subnetwork: strings.TrimSpace(netAndSubnet[1]),
		})
	}
	return additionalNodeNetworks, nil
}
