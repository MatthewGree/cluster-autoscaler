/*
Copyright 2024 The Kubernetes Authors.

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

package store

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	apiv1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuretesting "k8s.io/component-base/featuregate/testing"
	schedulerinterface "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/clustersnapshot"
	"sigs.k8s.io/cluster-autoscaler/pkg/simulator/framework"
	"sigs.k8s.io/cluster-autoscaler/pkg/utils/test"
)

const (
	hostnameTopologyKey = "kubernetes.io/hostname"
	zoneTopologyKey     = "topology.kubernetes.io/zone"
)

var (
	affinityLabels     = map[string]string{"app": "affinity-target"}
	antiAffinityLabels = map[string]string{"app": "anti-affinity-target"}
)

func buildTestNodeInfo(name string) *framework.NodeInfo {
	return framework.NewNodeInfo(test.BuildTestNode(name, 4000, 8*1024*1024*1024), nil)
}

func extractNodeInfoSummary(list []schedulerinterface.NodeInfo) map[string][]string {
	res := make(map[string][]string, len(list))
	for _, ni := range list {
		pods := make([]string, 0, len(ni.GetPods()))
		for _, p := range ni.GetPods() {
			pods = append(pods, fmt.Sprintf("%s/%s:%d:%d",
				p.GetPod().Namespace,
				p.GetPod().Name,
				ni.GetRequested().GetMilliCPU(),
				ni.GetRequested().GetMemory()))
		}
		sort.Strings(pods)
		res[ni.Node().Name] = pods
	}
	return res
}

func nodeNamesInOrder(list []schedulerinterface.NodeInfo) []string {
	names := make([]string, 0, len(list))
	for _, ni := range list {
		names = append(names, ni.Node().Name)
	}
	return names
}

func extractNodeNames(list []schedulerinterface.NodeInfo) []string {
	names := nodeNamesInOrder(list)
	sort.Strings(names)
	return names
}

func assertDeltaMatchesBasic(t *testing.T, step string, delta *DeltaSnapshotStore, basic *BasicSnapshotStore, pvcKeys []string) {
	t.Helper()

	deltaList, err := delta.NodeInfos().List()
	assert.NoError(t, err, step)
	basicList, err := basic.NodeInfos().List()
	assert.NoError(t, err, step)

	assert.ElementsMatch(t, extractNodeNames(basicList), extractNodeNames(deltaList), "NodeInfos().List() node names mismatch at %s", step)
	assert.Equal(t, extractNodeInfoSummary(basicList), extractNodeInfoSummary(deltaList), "NodeInfos().List() summary mismatch at %s", step)

	// Verify each node via Get(nodeName).
	for _, bni := range basicList {
		dni, err := delta.NodeInfos().Get(bni.Node().Name)
		assert.NoError(t, err, "%s: Get(%s)", step, bni.Node().Name)
		assert.Equal(t, bni.GetRequested().GetMilliCPU(), dni.GetRequested().GetMilliCPU(), "%s: MilliCPU for %s", step, bni.Node().Name)
		assert.Equal(t, bni.GetRequested().GetMemory(), dni.GetRequested().GetMemory(), "%s: Memory for %s", step, bni.Node().Name)
	}

	// Verify the pod-derived node lists.
	derivedLists := []struct {
		name string
		list func(schedulerinterface.NodeInfoLister) ([]schedulerinterface.NodeInfo, error)
	}{
		{"HavePodsWithAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithAffinityList},
		{"HavePodsWithRequiredAntiAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithRequiredAntiAffinityList},
		{"HavePodsWithRequiredNonHostScopedAntiAffinityList", schedulerinterface.NodeInfoLister.HavePodsWithRequiredNonHostScopedAntiAffinityList},
	}
	for _, dl := range derivedLists {
		deltaDerived, err := dl.list(delta.NodeInfos())
		assert.NoError(t, err, step)
		basicDerived, err := dl.list(basic.NodeInfos())
		assert.NoError(t, err, step)
		assert.Equal(t, extractNodeNames(basicDerived), extractNodeNames(deltaDerived), "%s: %s mismatch", step, dl.name)
	}

	// Verify IsPVCUsedByPods.
	for _, key := range pvcKeys {
		assert.Equal(t, basic.IsPVCUsedByPods(key), delta.IsPVCUsedByPods(key), "%s: IsPVCUsedByPods(%s) mismatch", step, key)
	}
}

// differentialStep is a single operation applied to both a DeltaSnapshotStore and a BasicSnapshotStore.
type differentialStep struct {
	name string
	op   func(s clustersnapshot.ClusterSnapshotStore) error
}

func forkStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { s.Fork(); return nil }}
}

func revertStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { s.Revert(); return nil }}
}

func commitStep(name string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { return s.Commit() }}
}

func addNodeStep(name string, nodeInfo func() *framework.NodeInfo) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error { return s.StoreNodeInfo(nodeInfo()) }}
}

func removeNodeStep(name string, nodeName string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error {
		return s.RemoveNodeInfo(context.Background(), nodeName)
	}}
}

func addPodStep(name string, pod *apiv1.Pod, nodeName string) differentialStep {
	return differentialStep{name: name, op: func(s clustersnapshot.ClusterSnapshotStore) error {
		return s.StorePodInfo(framework.NewPodInfo(pod, nil), nodeName)
	}}
}

func TestDeltaSnapshotStoreDifferentialMultiDepth(t *testing.T) {
	// Populates NodeInfo.PodsWithRequiredNonHostScopedAntiAffinity, so that
	// HavePodsWithRequiredNonHostScopedAntiAffinityList is exercised.
	featuretesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InterPodAffinityHostnameFastPath, true)
	pvcKeys := []string{"default/pvc-0", "default/pvc-1", "default/pvc-2", "default/pvc-3"}

	var steps []differentialStep

	// Depth 0: add 10 base nodes and schedule pods onto some of them.
	for i := 0; i < 10; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		steps = append(steps, addNodeStep("depth0-add-"+nodeName, func() *framework.NodeInfo { return buildTestNodeInfo(nodeName) }))
	}
	for i := 0; i < 5; i++ {
		opts := []func(*apiv1.Pod){test.WithPVC(fmt.Sprintf("pvc-%d", i%2))}
		if i%2 == 0 {
			opts = append(opts, test.WithPodAffinity(affinityLabels, hostnameTopologyKey))
		}
		if i%3 == 0 {
			opts = append(opts, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey))
		}
		pod := test.BuildTestPod(fmt.Sprintf("pod-d0-%d", i), 500, 1024, opts...)
		steps = append(steps, addPodStep(fmt.Sprintf("depth0-addpod-%d", i), pod, fmt.Sprintf("node-%d", i)))
	}

	// Depth 1: modify base nodes, add several nodes and remove one of them, without deleting base nodes.
	steps = append(steps, forkStep("fork-to-depth1"))
	for i := 5; i < 9; i++ {
		opts := []func(*apiv1.Pod){test.WithPVC("pvc-2")}
		if i == 5 {
			opts = append(opts, test.WithPodAffinity(affinityLabels, hostnameTopologyKey))
		}
		if i == 6 {
			opts = append(opts, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey))
		}
		pod := test.BuildTestPod(fmt.Sprintf("pod-d1-%d", i), 300, 512, opts...)
		steps = append(steps, addPodStep(fmt.Sprintf("depth1-addpod-%d", i), pod, fmt.Sprintf("node-%d", i)))
	}
	steps = append(steps, addPodStep("depth1-addpod-zone-anti-affinity-9",
		test.BuildTestPod("pod-d1-zone", 200, 256, test.WithPodAntiAffinity(antiAffinityLabels, zoneTopologyKey)), "node-9"))
	for i := 10; i < 14; i++ {
		nodeName := fmt.Sprintf("node-%d", i)
		steps = append(steps, addNodeStep("depth1-add-"+nodeName, func() *framework.NodeInfo { return buildTestNodeInfo(nodeName) }))
	}
	steps = append(steps,
		// Removes a node added in the same layer.
		removeNodeStep("depth1-remove-added-node-11", "node-11"),
		differentialStep{name: "depth1-removepod-0", op: func(s clustersnapshot.ClusterSnapshotStore) error {
			return s.RemovePodInfo("default", "pod-d0-0", "node-0")
		}},
	)

	// Depth 2: modify a node added in the parent, then delete and re-add nodes.
	steps = append(steps,
		forkStep("fork-to-depth2"),
		addPodStep("depth2-addpod-parent-added-node-13",
			test.BuildTestPod("pod-d2-13", 400, 512, test.WithPodAntiAffinity(antiAffinityLabels, zoneTopologyKey), test.WithPVC("pvc-0")), "node-13"),
		removeNodeStep("depth2-remove-node-2", "node-2"),
		// Re-adds a node deleted in the same layer.
		addNodeStep("depth2-readd-node-2", func() *framework.NodeInfo {
			node := test.BuildTestNode("node-2", 8000, 16*1024*1024*1024)
			pod := test.BuildTestPod("pod-readded-2", 1000, 2048,
				test.WithPodAffinity(affinityLabels, hostnameTopologyKey),
				test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey),
				test.WithPVC("pvc-3"))
			return framework.NewNodeInfo(node, nil, framework.NewPodInfo(pod, nil))
		}),
		removeNodeStep("depth2-remove-node-3", "node-3"),
		// Removes a node added in the parent layer.
		removeNodeStep("depth2-remove-parent-added-node-12", "node-12"),
	)

	// Depth 3: make changes and revert them.
	steps = append(steps,
		forkStep("fork-to-depth3"),
		addPodStep("depth3-addpod-node-10", test.BuildTestPod("pod-d3-temp", 700, 1024, test.WithPodAntiAffinity(antiAffinityLabels, hostnameTopologyKey), test.WithPVC("pvc-1")), "node-10"),
		revertStep("revert-depth3-to-depth2"),
	)

	// Depth 3 again: re-add a node deleted in the parent layer and commit it.
	steps = append(steps,
		forkStep("fork-to-depth3-again"),
		addNodeStep("depth3-readd-parent-deleted-node-3", func() *framework.NodeInfo { return buildTestNodeInfo("node-3") }),
		addPodStep("depth3-addpod-node-3", test.BuildTestPod("pod-d3-3", 100, 128), "node-3"),
		commitStep("commit-depth3-to-depth2"),
	)

	// Commit all the way down.
	steps = append(steps,
		commitStep("commit-depth2-to-depth1"),
		commitStep("commit-depth1-to-depth0"),
	)

	delta := NewDeltaSnapshotStore()
	basic := NewBasicSnapshotStore()
	sawNonHostScopedAntiAffinity := false
	for _, step := range steps {
		errDelta := step.op(delta)
		errBasic := step.op(basic)
		assert.Equal(t, errBasic != nil, errDelta != nil, "%s: error presence mismatch (delta=%v, basic=%v)", step.name, errDelta, errBasic)
		assertDeltaMatchesBasic(t, step.name, delta, basic, pvcKeys)
		if list, _ := basic.NodeInfos().HavePodsWithRequiredNonHostScopedAntiAffinityList(); len(list) > 0 {
			sawNonHostScopedAntiAffinity = true
		}
	}
	assert.True(t, sawNonHostScopedAntiAffinity, "the scenario should exercise non-host-scoped anti-affinity")
}
