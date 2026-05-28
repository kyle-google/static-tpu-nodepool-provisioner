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

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/copied/api/v1beta1"
	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/cloud"
	"github.com/GoogleCloudPlatform/ai-on-gke/static-np-provisioner/internal/utils"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	ConfigMapName = "tpu-provisioner-static-nodepools-config"
)

type gscSubblock struct {
	Block          string                      `yaml:"block"`
	Subblocks      string                      `yaml:"subblocks"`
	NodepoolConfig *cloud.StaticNodePoolConfig `yaml:"nodepoolConfig"`
}

type reservation struct {
	Name         string        `yaml:"name"`
	GscSubblocks []gscSubblock `yaml:"gscSubblocks"`
}

// StaticNodepoolReconciler reconciles static nodepools based on a configmap.
type StaticNodepoolReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder

	Provider                        cloud.Provider
	StaticNodepoolCreateConcurrency int
	StaticNodepoolDeleteConcurrency int
	StaticNodepoolCreateTimeout     time.Duration
	Namespace                       string
}

//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=nodes,verbs=list;watch
//+kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch

func (r *StaticNodepoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	lg := ctrllog.FromContext(ctx)

	lg.V(3).Info("Reconciling static nodepools")

	var cm corev1.ConfigMap
	if err := r.Get(ctx, req.NamespacedName, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			// If the configmap is not found, do nothing.
			lg.Info("Static nodepools configmap not found. Taking no action.", "configmap", req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get configmap %s: %w", req.NamespacedName.String(), err)
	}

	reservationsYAML, ok := cm.Data["reservations"]
	if !ok {
		lg.Info("No 'reservations' key in configmap. Skipping reconciliation.", "configmap", req.NamespacedName.String())
		return ctrl.Result{}, nil
	}

	var reservations []reservation
	if err := yaml.Unmarshal([]byte(reservationsYAML), &reservations); err != nil {
		lg.Error(err, "failed to unmarshal reservations from configmap", "configmap", req.NamespacedName.String())
		return ctrl.Result{}, nil
	}

	var defaultConfig *cloud.StaticNodePoolConfig
	defaultNodepoolConfigYAML, ok := cm.Data["defaultNodepoolConfig"]
	if ok {
		var config cloud.StaticNodePoolConfig
		if err := yaml.Unmarshal([]byte(defaultNodepoolConfigYAML), &config); err != nil {
			lg.Error(err, "failed to unmarshal defaultNodepoolConfig from configmap", "configmap", req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		defaultConfig = &config
	}

	var hasAnyConfig bool
	if defaultConfig != nil {
		hasAnyConfig = true
	} else {
		for _, res := range reservations {
			for _, subblock := range res.GscSubblocks {
				if subblock.NodepoolConfig != nil {
					hasAnyConfig = true
					break
				}
			}
			if hasAnyConfig {
				break
			}
		}
	}

	if !hasAnyConfig {
		lg.Info("No default or subblock-level nodepoolConfig found in configmap. Skipping reconciliation.", "configmap", req.NamespacedName.String())
		return ctrl.Result{}, nil
	}

	// List nodepools that should exist in the cluster based on the configmap.
	desiredNodePools, err := r.constructDesiredNodePools(reservations, defaultConfig)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to get desired nodepools from config: %w", err)
	}

	// List all static nodepools that currently exist in the cluster.
	existingNodePools, err := r.Provider.ListNodePools()
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list existing nodepools: %w", err)
	}

	toCreate, toDeleteMissing, toDeleteUpdate, toDeleteError, err := r.Provider.DiffStaticNodePools(existingNodePools, desiredNodePools)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to diff nodepools: %w", err)
	}

	// Track skipped deletes/recreates when nodepool is in use by a slice
	var skippedCapacity map[string]bool
	skippedUpdates := make(map[string]bool)

	if len(toDeleteMissing) > 0 || len(toDeleteUpdate) > 0 {
		// List all Slices
		var sliceList v1beta1.SliceList
		if err := r.List(ctx, &sliceList); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing slices: %w", err)
		}

		// List all Nodes
		var nodeList corev1.NodeList
		if err := r.List(ctx, &nodeList, client.MatchingLabels{cloud.LabelTPUProvisionerStaticNodepool: "true"}); err != nil {
			return ctrl.Result{}, fmt.Errorf("listing nodes: %w", err)
		}

		inUseNodepools := GetInUseNodepools(sliceList.Items, nodeList.Items)

		existingMap := make(map[string]cloud.NodePoolRef)
		for _, np := range existingNodePools {
			existingMap[np.Name] = np
		}

		// Filter out toDelete nodepools that are locked by a Slice.
		filterLocked := func(pools []string, actionName string, trackSkippedUpdates bool) []string {
			var filtered []string
			for _, name := range pools {
				if inUseNodepools[name] {
					lg.Info("Skipping "+actionName+" of static nodepool because it is in use by a Slice", "nodepool", name)
					if trackSkippedUpdates {
						skippedUpdates[name] = true
					}
					if np, ok := existingMap[name]; ok {
						skippedCapacity[np.SubblockName] = true
					}
					continue
				}
				filtered = append(filtered, name)
			}
			return filtered
		}

		skippedCapacity = make(map[string]bool)
		toDeleteMissing = filterLocked(toDeleteMissing, "deletion", false)
		toDeleteUpdate = filterLocked(toDeleteUpdate, "recreation (update)", true)
	}

	// Filter toCreate
	if len(toCreate) > 0 {
		var filteredCreate []*cloud.DesiredStaticNodePool
		for _, desired := range toCreate {
			// If a nodepool update was skipped, we must also skip the corresponding creation.
			if skippedUpdates[desired.Name] {
				lg.Info("Skipping creation of static nodepool because deletion of existing version was skipped (in use)", "nodepool", desired.Name)
				continue
			}
			// If the target capacity is held by ANY skipped nodepool (even if named differently), we must skip creation.
			if skippedCapacity != nil && skippedCapacity[desired.SubblockToConsume] {
				lg.Info("Skipping creation of static nodepool because the target capacity is held by an existing nodepool that cannot be deleted (in use)", "nodepool", desired.Name, "subblock", desired.SubblockToConsume)
				continue
			}
			filteredCreate = append(filteredCreate, desired)
		}
		toCreate = filteredCreate
	}

	if len(toDeleteMissing) > 0 {
		lg.Info("Deleting static nodepools not found in config", "nodepools", toDeleteMissing)
		errs := r.Provider.DeleteStaticNodePools(ctx, toDeleteMissing, r.StaticNodepoolDeleteConcurrency, &cm, "static nodepool not in config")
		if len(errs) > 0 {
			return ctrl.Result{}, fmt.Errorf("failed to delete some static nodepools: %v", errs)
		}
	}

	if len(toDeleteError) > 0 {
		lg.Info("Deleting static nodepools in ERROR state to retry", "nodepools", toDeleteError)
		errs := r.Provider.DeleteStaticNodePools(ctx, toDeleteError, r.StaticNodepoolDeleteConcurrency, &cm, "static nodepool in error state")
		if len(errs) > 0 {
			return ctrl.Result{}, fmt.Errorf("failed to delete some static nodepools: %v", errs)
		}
	}

	if len(toDeleteUpdate) > 0 {
		lg.Info("Deleting static nodepools to recreate (config changed)", "nodepools", toDeleteUpdate)
		errs := r.Provider.DeleteStaticNodePools(ctx, toDeleteUpdate, r.StaticNodepoolDeleteConcurrency, &cm, "static nodepool config changed")
		if len(errs) > 0 {
			return ctrl.Result{}, fmt.Errorf("failed to delete some static nodepools: %v", errs)
		}
	}

	if len(toCreate) > 0 {
		lg.Info("Creating static nodepools", "nodepools", toCreate)
		createCtx, cancel := context.WithTimeout(ctx, r.StaticNodepoolCreateTimeout)
		defer cancel()
		if err := r.Provider.EnsureStaticNodePools(createCtx, toCreate, r.StaticNodepoolCreateConcurrency, &cm); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to ensure static nodepools: %w", err)
		}
	}

	return ctrl.Result{}, nil
}

func (r *StaticNodepoolReconciler) constructDesiredNodePools(reservations []reservation, defaultConfig *cloud.StaticNodePoolConfig) ([]*cloud.DesiredStaticNodePool, error) {
	var desiredNodePools []*cloud.DesiredStaticNodePool

	for _, reservation := range reservations {
		for _, gscSubblock := range reservation.GscSubblocks {
			start, end, err := cloud.ParseSubBlocks(gscSubblock.Subblocks)
			if err != nil {
				return nil, fmt.Errorf("parsing subblocks for gscSubblock %s: %w", gscSubblock.Block, err)
			}

			// Determine config for this subblock.
			var subblockConfig *cloud.StaticNodePoolConfig
			if gscSubblock.NodepoolConfig != nil {
				subblockConfig = mergeConfig(defaultConfig, gscSubblock.NodepoolConfig)
			} else {
				subblockConfig = defaultConfig
			}

			if subblockConfig == nil {
				return nil, fmt.Errorf("no nodepool config specified in default or for gscSubblock %s", gscSubblock.Block)
			}

			for i := start; i <= end; i++ {
				nodePoolID := utils.SetNodePoolName(subblockConfig.NodepoolPrefix, gscSubblock.Block, i)
				subblockName := fmt.Sprintf("%s-subblock-%04d", gscSubblock.Block, i)
				subblockToConsume := fmt.Sprintf("projects/%s/reservations/%s/reservationBlocks/%s/reservationSubBlocks/%s", r.Provider.ProjectID(), reservation.Name, gscSubblock.Block, subblockName)

				desiredNodePools = append(desiredNodePools, &cloud.DesiredStaticNodePool{
					Name:              nodePoolID,
					SubblockToConsume: subblockToConsume,
					Config:            subblockConfig,
				})
			}
		}
	}

	return desiredNodePools, nil
}

func mergeConfig(global, subblock *cloud.StaticNodePoolConfig) *cloud.StaticNodePoolConfig {
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

// SetupWithManager sets up the controller with the Manager.
func (r *StaticNodepoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.ConfigMap{}).
		Watches(
			&v1beta1.Slice{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
				return []reconcile.Request{
					{
						NamespacedName: types.NamespacedName{
							Name:      ConfigMapName,
							Namespace: r.Namespace,
						},
					},
				}
			}),
		).
		Complete(r)
}

// GetInUseNodepools determines which nodepools are currently in use by Slices.
// It returns a map where the key is the nodepool name and the value is true if it's in use.
func GetInUseNodepools(slices []v1beta1.Slice, nodes []corev1.Node) map[string]bool {
	usedPartitions := make(map[string]bool)
	for _, slice := range slices {
		for _, p := range slice.Spec.PartitionIds {
			usedPartitions[p] = true
		}
	}

	inUseNodepools := make(map[string]bool)
	for _, node := range nodes {
		npName, ok := node.Labels[cloud.GKENodePoolNameLabel]
		if !ok {
			continue
		}

		for k, v := range node.Labels {
			// Check for labels like "cloud.google.com/gke-tpu-partition-*-id"
			if utils.IsPartitionIDLabel(k) {
				if usedPartitions[v] {
					inUseNodepools[npName] = true
					break // Found a used partition on this node, so the nodepool is in use.
				}
			}
		}
	}
	return inUseNodepools
}
