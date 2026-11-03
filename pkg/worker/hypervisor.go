// Package worker is the shared worker kit. Each OS-specific worker
// (worker-linux, worker-windows, worker-macos) implements Hypervisor for its
// platform and gets everything else from this package: the warm pool of VMs,
// image verification and, later, the controller connection and job runner.
//
// The rule every part of the kit follows: a VM runs exactly one job and is
// destroyed afterwards. Nothing is ever reused between jobs.
package worker

import (
	"context"
	"net"
)

// Hypervisor is the one interface an OS-specific worker implements.
type Hypervisor interface {
	// Name identifies the driver, e.g. "firecracker", "hyperv" or "vz".
	Name() string

	// Create makes a new VM as a copy-on-write clone of a verified golden
	// image. The VM is not started. Create should be fast (milliseconds):
	// it must not copy the whole image.
	Create(ctx context.Context, spec VMSpec) (VM, error)
}

// VM is one job VM.
type VM interface {
	// ID is unique for the lifetime of the worker.
	ID() string

	// Start boots the VM and returns once the guest agent is reachable.
	Start(ctx context.Context) error

	// DialAgent connects to the guest agent over a host-only channel such
	// as vsock or virtio-serial. The guest needs no network for this.
	DialAgent(ctx context.Context) (net.Conn, error)

	// Destroy stops the VM and deletes everything it wrote. It must be safe
	// to call more than once and on a VM that never started.
	Destroy(ctx context.Context) error
}

// VMSpec describes the VM to create.
type VMSpec struct {
	Image    Image
	CPUs     int
	MemoryMB int
	Network  NetworkPolicy
}

// NetworkPolicy controls what the guest can reach. The host owns the guest's
// virtual network, so it is enforced the same way on every OS.
type NetworkPolicy struct {
	// Restricted limits egress to AllowHosts. Otherwise the guest has
	// normal outbound access.
	Restricted bool
	AllowHosts []string
}
