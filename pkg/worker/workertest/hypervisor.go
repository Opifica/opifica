// Package workertest provides a fake Hypervisor for testing code built on the
// worker kit, without real VMs.
package workertest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/opifica/opifica/internal/agent"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/pkg/worker"
)

// Hypervisor is an in-memory worker.Hypervisor. It records how many VMs are
// alive at once, so tests can check capacity limits.
type Hypervisor struct {
	// BootDelay is how long Start takes.
	BootDelay time.Duration
	// FailStart, if set, is called on every Start; a non-nil error fails it.
	FailStart func(spec worker.VMSpec) error
	// Agent runs a real agent for every VM, on the host, in a fresh work
	// directory that is deleted with the VM. Jobs then really run, but with
	// NO isolation: for tests and development only.
	Agent bool

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

	// With Hypervisor.Agent:
	root      string
	agentAddr string
	stopAgent func()
}

// Root is the VM's agent work directory (with Hypervisor.Agent).
func (v *VM) Root() string { return v.root }

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
	if v.hv.Agent {
		if err := v.startAgent(); err != nil {
			return err
		}
	}
	v.started = true
	return nil
}

func (v *VM) startAgent() error {
	root, err := os.MkdirTemp("", "opifica-vm-")
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		os.RemoveAll(root)
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan struct{})
	go func() {
		server.Serve(ctx, ln, agent.Handler(&agent.Server{Root: root, Log: quiet}), quiet)
		close(done)
	}()
	v.root, v.agentAddr = root, ln.Addr().String()
	v.stopAgent = func() {
		cancel()
		<-done
		os.RemoveAll(root)
	}
	return nil
}

// DialAgent implements worker.VM with an in-memory pipe.
func (v *VM) DialAgent(ctx context.Context) (net.Conn, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.started || v.gone {
		return nil, errors.New("vm not running")
	}
	if v.agentAddr != "" {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", v.agentAddr)
	}
	client, srv := net.Pipe()
	srv.Close()
	return client, nil
}

// Destroy implements worker.VM. It is safe to call more than once.
func (v *VM) Destroy(context.Context) error {
	v.mu.Lock()
	already := v.gone
	v.gone = true
	stop := v.stopAgent
	v.mu.Unlock()
	if already {
		return nil
	}
	if stop != nil {
		stop()
	}
	v.hv.mu.Lock()
	defer v.hv.mu.Unlock()
	delete(v.hv.alive, v.id)
	v.hv.destroyed++
	return nil
}
