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

// func CollectAll(ctx context.Context, c client.Client) (map[string]Result, error) {
// 	var pods corev1.PodList
// 	if err := c.List(ctx, &pods, client.InNamespace("gpu-operator")); err != nil {
// 		return nil, err
// 	}

// 	out := map[string]Result{}
// 	for _, p := range pods.Items {
// 		if !strings.HasPrefix(p.Name, "nvidia-dcgm-exporter") ||
// 			p.Status.Phase != corev1.PodRunning ||
// 			p.Status.PodIP == "" {
// 			continue
// 		}
// 		r, err := scrapePod(p.Status.PodIP, p.Spec.NodeName)
// 		if err != nil {
// 			log.Error(err, "scrape failed",
// 				"pod", p.Name, "ip", p.Status.PodIP)
// 			continue
// 		}
// 		out[p.Spec.NodeName] = *r
// 	}
// 	return out, nil
// }

func CollectAll(ctx context.Context, c client.Client) (map[string]Result, error) {
    var nodes corev1.NodeList
    if err := c.List(ctx, &nodes); err != nil {
        return nil, err
    }

    var pods corev1.PodList
    if err := c.List(ctx, &pods); err != nil {
        return nil, err
    }

    out := map[string]Result{}
    var dcgmInfo []string
    var allNodeNames []string

    for _, n := range nodes.Items {
        allNodeNames = append(allNodeNames, n.Name)
    }

    for _, p := range pods.Items {
        if strings.HasPrefix(p.Name, "nvidia-dcgm-exporter") {
            info := fmt.Sprintf("[%s](IP:%s on Node:%s Status:%s)", 
                p.Name, p.Status.PodIP, p.Spec.NodeName, p.Status.Phase)
            dcgmInfo = append(dcgmInfo, info)

            if p.Status.Phase == corev1.PodRunning && p.Status.PodIP != "" {
                r, err := scrapePod(p.Status.PodIP, p.Spec.NodeName)
                if err == nil {
                    out[p.Spec.NodeName] = *r
                }
            }
        }
    }

    // log.Info("=== GPU Operator Startup Discovery ===")
    // log.Info("Nodes found in cluster", "list", allNodeNames)
    if len(dcgmInfo) > 0 {
        // log.Info("DCGM Exporters found", "details", dcgmInfo)
    } else {
        log.Info("WARNING: No DCGM Exporters found in any namespace!")
    }
    // log.Info("======================================")

    return out, nil
}

/* -------------------------------------------------------------------------- */
/*                                internals                                   */
/* -------------------------------------------------------------------------- */

func scrapePod(ip, node string) (*Result, error) {
    // 1. กำหนด Timeout (เช่น 5 วินาที) เพื่อไม่ให้ Operator ค้างถ้า Network มีปัญหา
    client := &http.Client{
        Timeout: 5 * time.Second,
    }

    url := "http://" + ip + ":9400/metrics"
    log.Info("📡 Scraping metrics", "node", node, "url", url)

    // 2. ใช้ client.Get แทน http.Get
    resp, err := client.Get(url)
    if err != nil {
        // จะโชว์ใน Log ทันทีถ้า Connection Refused หรือ Timeout
        log.Error(err, "❌ Failed to connect to DCGM exporter", "node", node, "ip", ip)
        return nil, err
    }
    defer resp.Body.Close()

    // 3. เช็ค HTTP Status Code
    if resp.StatusCode != http.StatusOK {
        errStatus := fmt.Errorf("bad status: %s", resp.Status)
        log.Error(errStatus, "❌ DCGM exporter returned error status", "node", node)
        return nil, errStatus
    }

    // 4. ส่งไป Parse
    res, err := parseMetrics(resp.Body, node)
    if err != nil {
        log.Error(err, "❌ Failed to parse metrics content", "node", node)
        return nil, err
    }

    log.Info("✅ Successfully scraped metrics", "node", node, "gpu_util", res.GPUUtil)
    return res, nil
}

func parseMetrics(r io.Reader, node string) (*Result, error) {
    sc := bufio.NewScanner(r)
    res := Result{GPUUtil: -1, MemUtil: -1, XID: 0} 
    foundAny := false

    for sc.Scan() {
        line := sc.Text()
        if strings.HasPrefix(line, "DCGM_FI_DEV_GPU_UTIL") {
            res.GPUUtil = parseFloat(line)
            foundAny = true
        } else if strings.HasPrefix(line, "DCGM_FI_DEV_MEM_COPY_UTIL") {
            res.MemUtil = parseFloat(line)
            foundAny = true
        } else if strings.HasPrefix(line, "DCGM_FI_DEV_XID_ERRORS") {
            res.XID = parseInt(line)
        }
    }

    if !foundAny || res.GPUUtil == -1 {
        return nil, fmt.Errorf("essential metrics (GPU Util) not found for %s", node)
    }

    return &res, nil
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
