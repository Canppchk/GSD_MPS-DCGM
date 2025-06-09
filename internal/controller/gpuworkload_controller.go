/*
Copyright 2025.

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
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ppchk/dcgm-mps-operator/internal/window"
	// "github.com/ppchk/dcgm-mps-operator/pkg/common"
	"github.com/ppchk/dcgm-mps-operator/pkg/metrics"
)

var clog = logf.Log.WithName("mps-controller")

/* -------------------------------------------------------------------------- */
/*                              state machine                                 */
/* -------------------------------------------------------------------------- */

// three explicit modes for clarity
const (
    modeSchedule  = "schedule"
    modeScaledown = "scaledown"
    modeRecovery  = "recovery"
)

type NodeState struct {
	// schedule   – normal scheduling
    // scaledown  – XID-31/32 + 100 % GPU  ⇒ delete 10 % newest RUNNING pods
    // recovery   – ≤ 80 % GPU + at least one Pending pod(no-capacity)  ⇒ delete ONE oldest Pending pod each tick
	Mode      string    
	LastCheck time.Time // when we last evaluated scale-down loop
}

var (
	stateMu sync.RWMutex
	// exported so the webhook can see which nodes are in scaledown
	ControllerState = map[string]*NodeState{}
)

/* -------------------------------------------------------------------------- */
/*                       scoring constants (same as webhook)                  */
/* -------------------------------------------------------------------------- */

const (
	alpha       = 0.3
	beta        = 0.3
	gamma       = 0.4
	scoreCutoff = 0.3
)

/* -------------------------------------------------------------------------- */
/*                            POD   RECONCILER                                */
/* -------------------------------------------------------------------------- */

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;delete;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch

type PodReconciler struct{ client.Client }

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the GpuWorkload object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.19.1/pkg/reconcile

/* -------------------------------------------------------------------------- */
/*                                 Reconcile                                  */
/* -------------------------------------------------------------------------- */

func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	r.refreshNodeStates(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// we act only on our Pending, no-capacity pods
	if pod.Labels["mps-share"] != "true" ||
		pod.Status.Phase != corev1.PodPending ||
		pod.Spec.NodeSelector["no-capacity"] != "true" {
		return ctrl.Result{}, nil
	}

	/* 30-second global window — keep it so we don’t spam the API server */
    allMetrics, err := metrics.CollectAll(ctx, r.Client)
    if err != nil {
        return ctrl.Result{RequeueAfter: 10 * time.Second}, err
    }
    if !window.TryAdd(len(allMetrics)) {
        return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
    }

    return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}


// rebuild deletes <old> and spawns a single replacement
// named <old.Name>-<suffix> (no random suffix).
func (r *PodReconciler) rebuild( ctx context.Context, old *corev1.Pod, suffix string ) error {

    newName := strings.TrimSuffix(old.Name, "-"+suffix) + "-" + suffix

    // if that name already exists we are done
    var exists corev1.Pod
    if err := r.Get(ctx,
        client.ObjectKey{Namespace: old.Namespace, Name: newName}, &exists); err == nil {
        return nil
    }

    np := old.DeepCopy()
    np.ResourceVersion = ""
    np.UID = ""
    np.Name = newName
    np.GenerateName = ""           // <-- deterministic name

    if np.Spec.NodeSelector == nil {
        np.Spec.NodeSelector = map[string]string{}
    }
    if suffix == "recovery" {
        delete(np.Spec.NodeSelector, "no-capacity")
    }

    // delete the old object (ignore AlreadyDeleting / Gone)
    _ = r.Delete(ctx, old)

    // create the replacement, ignore AlreadyExists (racing controller)
    if err := r.Create(ctx, np); err != nil && !apierrors.IsAlreadyExists(err) {
        return err
    }
    clog.Info("rebuilt pod",
        "old", old.Name, "new", np.Name, "suffix", suffix)
    return nil
}


/* -------------------------------------------------------------------------- */
/*                        node state-machine helpers                          */
/* -------------------------------------------------------------------------- */

func (r *PodReconciler) updateState(ctx context.Context, node string, rst *metrics.Result) *NodeState {
    now := time.Now()

    stateMu.Lock()
    defer stateMu.Unlock()

    st, ok := ControllerState[node]
    if !ok {
        st = &NodeState{Mode: modeSchedule}
        ControllerState[node] = st
    }

	// clog.V(1).Info("probe",
    // "node", node,
    // "gpu%", rst.GPUUtil,
    // "hasPending?", r.hasNoCapPending(ctx),
    // "mode", st.Mode)

    switch st.Mode {
    case modeSchedule:
        // current score of the node
	    score := r.scoreNode(ctx, node, rst)

        // ---------- enter scaledown ----------
        if int(rst.GPUUtil) == 100 && rst.XID == 31 && r.detectErrorPod(ctx, node) && score < scoreCutoff {
        	st.Mode, st.LastCheck = "scaledown", now
        	clog.Info("enter scaledown",
			"node", node,
			"gpu%", rst.GPUUtil,
			"score", score)
        	go r.scaleDown10Percent(ctx, node)
			return st
        }
		
        // ---------- enter recovery ----------
		if (rst.GPUUtil != 100 || score > scoreCutoff) && r.hasNoCapPending(ctx)   {
			st.Mode, st.LastCheck = modeRecovery, now
			clog.Info("enter recovery", "node", node)
			go r.recoverPending(ctx, node)
			return st
		}

    case modeScaledown:
        score := r.scoreNode(ctx, node, rst)

        if score > scoreCutoff || rst.GPUUtil != 100 {
            st.Mode = modeSchedule
            clog.Info("exit scaledown",
                "node", node,
                "gpu%", rst.GPUUtil,
                "score", score)
        } else if now.Sub(st.LastCheck) >= 30*time.Second {
            st.LastCheck = now
            go r.scaleDown10Percent(ctx, node)
        }

    case modeRecovery:
        score := r.scoreNode(ctx, node, rst)

		if now.Sub(st.LastCheck) >= 10*time.Second {
			st.LastCheck = now

			// still under-utilised and still have pending → delete one more
			if (rst.GPUUtil != 100 || score > scoreCutoff) && r.hasNoCapPending(ctx)   {
				go r.recoverPending(ctx, node)
			} else if !(r.hasNoCapPending(ctx)){
				st.Mode = "schedule" // all clear
				clog.Info("exit recovery", "node", node)
			}
		}
    }
    return st
}

/* -------------------------- helper predicates ---------------------------- */

func (r *PodReconciler) detectErrorPod(ctx context.Context, node string) bool {
    var list corev1.PodList
    _ = r.List(ctx, &list,
        client.MatchingFields{"spec.nodeName": node},
        client.MatchingLabels{"mps-share": "true"},
    )
    for _, p := range list.Items {
        if p.Status.Phase == corev1.PodFailed {
            return true
        }
        for _, cs := range p.Status.ContainerStatuses {
            ifcs := cs.State
            if ifcs.Waiting != nil && ifcs.Waiting.Reason == "Error" {
                return true
            }
            if ifcs.Terminated != nil && ifcs.Terminated.Reason == "Error" {
                return true
            }
        }
    }
    return false
}

// hasNoCapPending returns true if the cluster still has at least one
// mps-share pod that
//   * is Pending,
//   * carries the "no-capacity" selector.
//   * is older than 1 minute (to give the scheduler a chance first).
func (r *PodReconciler) hasNoCapPending(ctx context.Context) bool {
    var pl corev1.PodList
    _ = r.List(ctx, &pl,
        client.MatchingLabels{"mps-share": "true"},
    )

    now := time.Now()
    for _, p := range pl.Items {
        if p.DeletionTimestamp != nil {                // already terminating
            continue
        }
        if p.Status.Phase == corev1.PodPending &&
           p.Spec.NodeSelector["no-capacity"] == "true" &&
           now.Sub(p.CreationTimestamp.Time) > 2 * time.Minute {  // NEW age check
            return true
        }
    }
    return false
}


// scoreNode replicates the webhook’s formula for a single node.
func (r *PodReconciler) scoreNode(ctx context.Context, node string, rst *metrics.Result) float64 {

	// how many MPS pods (Running+Pending) currently target this node?
	var pl corev1.PodList
	_ = r.List(ctx, &pl,
		client.MatchingFields{"spec.nodeName": node},
		client.MatchingLabels{"mps-share": "true"},
	)

	total := 0
	for _, p := range pl.Items {
		if p.DeletionTimestamp == nil &&
			(p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending) {
			total++
		}
	}

	return (1-rst.GPUUtil/100)*alpha +
		(1-rst.MemUtil/100)*beta +
		(4/float64(total+4))*gamma
}

/* -------------------------- bulk operations ------------------------------ */

func (r *PodReconciler) scaleDown10Percent(ctx context.Context, node string) {
    var list corev1.PodList
    _ = r.List(ctx, &list,
        client.MatchingFields{"spec.nodeName": node},
        client.MatchingLabels{"mps-share": "true"},
    )

    var running []corev1.Pod
    for _, p := range list.Items {
        if p.DeletionTimestamp != nil {           // already terminating
            continue
        }
        if strings.Contains(p.Name, "-recovery") { // <-- NEW guard
            continue                               // ignore recovery replicas
        }
        if p.Status.Phase == corev1.PodRunning {
            running = append(running, p)
        }
    }
    if len(running) == 0 {
        return
    }

    sort.Slice(running, func(i, j int) bool {
        return running[i].CreationTimestamp.After(
               running[j].CreationTimestamp.Time)
    })

    target := int(math.Ceil(float64(len(running)) * 0.10)) // ceil(10 %)
    for i := range target {
        if err := r.rebuild(ctx, &running[i],
            "resched"); err != nil {
            clog.Error(err, "scaledown rebuild failed", "pod", running[i].Name)
        } else {
            clog.Info("scaledown → Pending (resched)",
                "pod", running[i].Name, "node", node)
        }
    }
}



func (r *PodReconciler) recoverPending(ctx context.Context, node string) {
    if !window.TryAdd(1) {                // global 30-s window
        return
    }

    var list corev1.PodList
    _ = r.List(ctx, &list, client.MatchingLabels{"mps-share": "true"})

    sort.Slice(list.Items, func(i, j int) bool {
        return list.Items[i].CreationTimestamp.Before(
               &list.Items[j].CreationTimestamp)   // oldest first
    })

    for _, p := range list.Items {
        // ─── ignore pods that are already terminating … ───
        if p.DeletionTimestamp != nil {
            continue
        }
        // ─── …and Pods that already have deterministic recovery name ───
        if strings.HasSuffix(p.Name, "-recovery") { 
            continue
        }
        // still Pending and still has the guard selector?
        if p.Status.Phase == corev1.PodPending &&
           p.Spec.NodeSelector["no-capacity"] == "true" {

            // build a single <orig>-recovery pod and drop the guard selector
            if err := r.rebuild(ctx, &p,
                               "recovery"); err != nil { // <-- NEW
                clog.Error(err, "recovery rebuild failed", "pod", p.Name)
            } else {
                clog.Info("recovery → rescheduled",
                          "oldPod", p.Name, "nodeHint", node)
            }
            return
        }
    }
}




/* -------------------------------------------------------------------------- */
/*                         state refresh helper                               */
/* -------------------------------------------------------------------------- */

func (r *PodReconciler) refreshNodeStates(ctx context.Context) {
    all, err := metrics.CollectAll(ctx, r.Client)
    if err != nil {
        clog.Error(err, "metrics collection failed")
        return
    }
    for node, rst := range all {
        r.updateState(ctx, node, &rst)
    }
}

/* -------------------------------------------------------------------------- */
/*                            Manager wiring                                  */
/* -------------------------------------------------------------------------- */

func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
    if err := mgr.GetFieldIndexer().IndexField(
        context.Background(), &corev1.Pod{}, "spec.nodeName",
        func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} },
    ); err != nil {
        return err
    }
    return ctrl.NewControllerManagedBy(mgr).
        WithOptions(controller.Options{MaxConcurrentReconciles: 4}).
        For(&corev1.Pod{}).
        Named("mps-pod").
        Complete(r)
}
