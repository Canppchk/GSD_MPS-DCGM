package metrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var log = logf.Log.WithName("metrics")

/* -------------------------------------------------------------------------- */
/*                              public types                                  */
/* -------------------------------------------------------------------------- */

type Result struct {
	GPUUtil float64
	MemUtil float64
	XID     int
}

/* -------------------------------------------------------------------------- */
/*                         cluster-wide collection                             */
/* -------------------------------------------------------------------------- */

func CollectAll(ctx context.Context, c client.Client) (map[string]Result, error) {
	var pods corev1.PodList
	if err := c.List(ctx, &pods, client.InNamespace("gpu-operator")); err != nil {
		return nil, err
	}

	out := map[string]Result{}
	for _, p := range pods.Items {
		if !strings.HasPrefix(p.Name, "nvidia-dcgm-exporter") ||
			p.Status.Phase != corev1.PodRunning ||
			p.Status.PodIP == "" {
			continue
		}
		r, err := scrapePod(p.Status.PodIP, p.Spec.NodeName)
		if err != nil {
			log.Error(err, "scrape failed",
				"pod", p.Name, "ip", p.Status.PodIP)
			continue
		}
		out[p.Spec.NodeName] = *r
	}
	return out, nil
}

/* -------------------------------------------------------------------------- */
/*                                internals                                   */
/* -------------------------------------------------------------------------- */

func scrapePod(ip, node string) (*Result, error) {
	resp, err := http.Get("http://" + ip + ":9400/metrics") // #nosec G107
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return parseMetrics(resp.Body, node)
}

func parseMetrics(r io.Reader, node string) (*Result, error) {
	sc := bufio.NewScanner(r)
	res := Result{GPUUtil: -1, MemUtil: -1, XID: -1}
	for sc.Scan() {
		line := sc.Text()

		switch {
		case strings.HasPrefix(line, "DCGM_FI_DEV_GPU_UTIL"):
			res.GPUUtil = parseFloat(line)

		case strings.HasPrefix(line, "DCGM_FI_DEV_MEM_COPY_UTIL"):
			res.MemUtil = parseFloat(line)

		case strings.HasPrefix(line, "DCGM_FI_DEV_XID_ERRORS"):
			res.XID = parseInt(line)
		}

		if res.GPUUtil >= 0 && res.MemUtil >= 0 && res.XID >= 0 {
			return &res, nil
		}
	}
	return nil, fmt.Errorf("metrics not found for %s", node)
}

/* -------------------------------------------------------------------------- */
/*                                helpers                                     */
/* -------------------------------------------------------------------------- */

func parseFloat(line string) float64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return -1
	}
	v, _ := strconv.ParseFloat(fields[len(fields)-1], 64)
	return v
}

func parseInt(line string) int {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.Atoi(fields[len(fields)-1])
	return v
}
