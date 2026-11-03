// Package workertest provides a fake Hypervisor for testing code built on the
// worker kit, without real VMs.
package workertest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opifica/opifica/pkg/worker"
)

// Hypervisor is an in-memory worker.Hypervisor. It records how many VMs are
// alive at once, so tests can check capacity limits.
type Hypervisor struct {
	// BootDelay is how long Start takes.
	BootDelay time.Duration
	// FailStart, if set, is called on every Start; a non-nil error fails it.
	FailStart func(spec worker.VMSpec) error

	nextID    atomic.Int64
	mu        sync.Mutex
	alive     map[string]*VM
	maxAlive  int
	created   int
	destroyed int
}

var _ worker.Hypervisor = (*Hypervisor)(nil)

// Name implements worker.Hypervisor.
func (h *Hypervisor) Name() string { return "fake" }

// Create implements worker.Hypervisor. The VM counts as alive from Create
// until Destroy, like a real VM's disk and memory.
func (h *Hypervisor) Create(ctx context.Context, spec worker.VMSpec) (worker.VM, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	vm := &VM{id: fmt.Sprintf("vm-%d", h.nextID.Add(1)), spec: spec, hv: h}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.alive == nil {
		h.alive = map[string]*VM{}
	}
	h.alive[vm.id] = vm
	h.created++
	h.maxAlive = max(h.maxAlive, len(h.alive))
	return vm, nil
}

// Alive returns how many VMs exist right now.
func (h *Hypervisor) Alive() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.alive)
}

// MaxAlive returns the most VMs that ever existed at the same time.
func (h *Hypervisor) MaxAlive() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.maxAlive
}

// Counts returns how many VMs were created and destroyed in total.
func (h *Hypervisor) Counts() (created, destroyed int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.created, h.destroyed
}

// VM is a fake worker.VM.
type VM struct {
	id      string
	spec    worker.VMSpec
	hv      *Hypervisor
	mu      sync.Mutex
	started bool
	gone    bool
}

var _ worker.VM = (*VM)(nil)

// ID implements worker.VM.
func (v *VM) ID() string { return v.id }

// Spec returns the spec the VM was created with.
func (v *VM) Spec() worker.VMSpec { return v.spec }

// Start implements worker.VM.
func (v *VM) Start(ctx context.Context) error {
	if v.hv.FailStart != nil {
		if err := v.hv.FailStart(v.spec); err != nil {
			return err
		}
	}
	select {
	case <-time.After(v.hv.BootDelay):
	case <-ctx.Done():
		return ctx.Err()
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.gone {
		return errors.New("vm destroyed")
	}
	v.started = true
	return nil
}

// DialAgent implements worker.VM with an in-memory pipe.
func (v *VM) DialAgent(ctx context.Context) (net.Conn, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.started || v.gone {
		return nil, errors.New("vm not running")
	}
	client, server := net.Pipe()
	server.Close()
	return client, nil
}

// Destroy implements worker.VM. It is safe to call more than once.
func (v *VM) Destroy(context.Context) error {
	v.mu.Lock()
	already := v.gone
	v.gone = true
	v.mu.Unlock()
	if already {
		return nil
	}
	v.hv.mu.Lock()
	defer v.hv.mu.Unlock()
	delete(v.hv.alive, v.id)
	v.hv.destroyed++
	return nil
}
