// internal/window/window.go
package window

import (
	"sync"
	"time"
)
const WinFactor = 5 

const Length = 10 * time.Second   // rolling window

var (
	mu        sync.Mutex
	start     time.Time
	admitted  int
)

// TryAdd returns true if the caller MAY admit one more pod.
// gpuNodes is the current number of GPU nodes; the limit is 2*gpuNodes.
func TryAdd(gpuNodes int) bool {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now()
	if start.IsZero() || now.Sub(start) > Length {
		start = now
		admitted = 0
	}

	if admitted >= WinFactor*gpuNodes {
		return false
	}
	admitted++
	return true
}
