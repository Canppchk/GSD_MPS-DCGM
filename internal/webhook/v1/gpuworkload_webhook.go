package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	// "math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	ctrlstate "github.com/ppchk/dcgm-mps-operator/internal/controller"
	"github.com/ppchk/dcgm-mps-operator/pkg/common"
	"github.com/ppchk/dcgm-mps-operator/pkg/metrics"
	"github.com/ppchk/dcgm-mps-operator/internal/window"
)

/* -------------------------------------------------------------------------- */
/*                           MUTATING  POD  WEBHOOK                           */
/* -------------------------------------------------------------------------- */

var log = logf.Log.WithName("pod-mutator")

// scoring weights
const (
	alpha = 0.3
	beta  = 0.3
	gamma = 0.4
	scoreCutoff  = 0.3
	windowPeriod = 30 * time.Second
)


// data we need per node
type cand struct {
	node    string
	gpuUtil float64
	memUtil float64
	pods    int
	score   float64
}

// +kubebuilder:webhook:path=/mutate-v1-pod,mutating=true,failurePolicy=Fail,sideEffects=None,groups="",resources=pods,verbs=create,versions=v1,name=mpstrigger.ppchk.dev,admissionReviewVersions=v1
type PodMutator struct{ Client client.Client }

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch

var admitLock sync.Mutex      // we still serialize requests per-process

func (m *PodMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	admitLock.Lock()
	defer admitLock.Unlock()

	/* ────────────────── decode incoming pod ─────────────────── */
	var pod corev1.Pod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		return admission.Errored(400, err)
	}
	key := podKey(req, &pod)
	plog := log.WithValues("pod", key)

	/* skip pods that are not ours or already pinned */
	if pod.Labels["mps-share"] != "true" {
		plog.Info("skip - not an MPS workload")
		return admission.Allowed("not our pod")
	}
	if host, ok := pod.Spec.NodeSelector["kubernetes.io/hostname"]; ok && host != "" {
		plog.Info("skip - hostname already set", "node", host)
		return admission.Allowed("already scheduled")
	}

	/* ───────────── gather GPU nodes & metrics once ───────────── */
	var nodes corev1.NodeList
	if err := m.Client.List(ctx, &nodes, client.MatchingLabels{"nvidia.com/gpu.present": "true"}); err != nil {
		plog.Error(err, "list GPU nodes")
		return admission.Errored(500, err)
	}
	all, err := metrics.CollectAll(ctx, m.Client)
	if err != nil {
		plog.Error(err, "collect metrics")
		return admission.Errored(500, err)
	}

	/* ─────────────── build candidate list ──────────────── */
	var allPods corev1.PodList
	_ = m.Client.List(ctx, &allPods, client.MatchingLabels{"mps-share": "true"})

	var cands []cand
	for _, n := range nodes.Items {
		// 1. skip nodes you never use
		if _, banned := common.SkipNodes[n.Name]; banned {
			continue
		}

		// 2. skip nodes that are in “scaledown” mode
		if st, ok := ctrlstate.ControllerState[n.Name]; ok && st.Mode == "scaledown" {
			plog.Info("skip - node in scaledown", "node", n.Name)
			continue
		}

		rst, ok := all[n.Name]
		if !ok {
			continue // exporter not ready
		}

		podCnt := 0
		for _, p := range allPods.Items {
			if p.Spec.NodeName == n.Name &&
				p.DeletionTimestamp == nil &&
				(p.Status.Phase == corev1.PodRunning ||
					p.Status.Phase == corev1.PodPending) {
				podCnt++
			}
		}

		score := (1-rst.GPUUtil/100)*alpha +
			(1-rst.MemUtil/100)*beta +
			(4/float64(podCnt+4))*gamma

		log.Info("candidate",
			"node", n.Name,
			"gpuUtil%", rst.GPUUtil,
			"memUtil%", rst.MemUtil,
			"pods", podCnt,
			"score", fmt.Sprintf("%.4f", score))

		cands = append(cands, cand{n.Name, rst.GPUUtil, rst.MemUtil, podCnt, score})
	}

	/* ───────────── decision section ───────────── */
	if len(cands) == 0 {
		// no healthy GPU node
		markNoCapacity(&pod)
		plog.Info("no GPU exporter reachable - Pending")
		return patchPod(req, &pod)
	}

	// best score
	best := cands[0]
	for _, c := range cands[1:] {
		if c.score > best.score {
			best = c
		}
	}

	if best.score < scoreCutoff {
		markNoCapacity(&pod)
		plog.Info("all scores below cutoff - Pending",
			"bestScore", fmt.Sprintf("%.4f", best.score))
		return patchPod(req, &pod)
	}

	if !window.TryAdd(len(nodes.Items)) {
		limit := window.WinFactor * len(nodes.Items)
		markNoCapacity(&pod)
		plog.Info("rate-limit window full - Pending",
			"limit", limit)
		return patchPod(req, &pod)
	}


	/* ───────────── schedule! ───────────── */
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	delete(pod.Spec.NodeSelector, "no-capacity")
	pod.Spec.NodeSelector["kubernetes.io/hostname"] = best.node

	plog.Info("selected",
		"selected-node", best.node,
		"score", fmt.Sprintf("%.4f", best.score))

	// IPC + root
	pod.Spec.HostIPC = true
	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	zero := int64(0)
	pod.Spec.SecurityContext.RunAsUser = &zero

	return patchPod(req, &pod)
}

/* -------------------------------------------------------------------------- */
/*                               helpers                                      */
/* -------------------------------------------------------------------------- */

func markNoCapacity(p *corev1.Pod) {
	if p.Spec.NodeSelector == nil {
		p.Spec.NodeSelector = map[string]string{}
	}
	p.Spec.NodeSelector["no-capacity"] = "true"
}

func patchPod(req admission.Request, p *corev1.Pod) admission.Response {
	raw, _ := json.Marshal(p)
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

func (m *PodMutator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().
		Register("/mutate-v1-pod", &admission.Webhook{Handler: m})
	return nil
}

func podKey(req admission.Request, pod *corev1.Pod) client.ObjectKey {
	if req.Name != "" {
		return client.ObjectKey{Namespace: req.Namespace, Name: req.Name}
	}
	if pod.GenerateName != "" {
		return client.ObjectKey{Namespace: req.Namespace, Name: pod.GenerateName + "*"}
	}
	return client.ObjectKey{Namespace: req.Namespace, Name: string(pod.UID)}
}