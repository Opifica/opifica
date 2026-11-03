// Command devworker is a worker for trying Opifica on one machine, without
// VMs. Every job gets a fresh directory and its own agent, but runs directly
// on this machine as the current user.
//
// THERE IS NO ISOLATION. Run only code you trust. Real workers live in the
// worker-linux, worker-windows and worker-macos repositories.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/opifica/opifica/internal/version"
	"github.com/opifica/opifica/pkg/worker"
	"github.com/opifica/opifica/pkg/worker/workertest"
)

const usage = `Usage: devworker <command> [options]

A worker WITHOUT VMs, for development: jobs run on this machine, unisolated.

Commands:
  join   Join a controller with a token from "controller token create"
  run    Run jobs
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch args[0] {
	case "join":
		return join(args[1:], stdout, stderr)
	case "run":
		return runJobs(args[1:], stderr)
	case "version":
		fmt.Fprintln(stdout, version.Version)
		return 0
	}
	fmt.Fprint(stderr, usage)
	return 2
}

type common struct {
	controller string
	stateDir   string
	capacity   int
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.controller, "controller", "https://localhost:8443", "controller URL")
	home, _ := os.UserHomeDir()
	fs.StringVar(&c.stateDir, "state-dir", filepath.Join(home, ".opifica-devworker"), "identity and repository mirrors")
	fs.IntVar(&c.capacity, "capacity", 2, "jobs to run at once")
}

func (c *common) pool() worker.PoolConfig {
	return worker.PoolConfig{
		Capacity: c.capacity,
		Images:   map[string]worker.Image{"default": {Name: "default"}},
		Warm:     map[string]int{"default": 1},
	}
}

func join(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("join", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	token := fs.String("token", "", "join token (required)")
	fp := fs.String("ca-fingerprint", "", "CA fingerprint printed with the token (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *token == "" || *fp == "" {
		fmt.Fprintln(stderr, "devworker: -token and -ca-fingerprint are required")
		return 2
	}
	id, err := worker.Join(context.Background(), worker.JoinConfig{
		ControllerURL: c.controller, Token: *token, CAFingerprint: *fp, StateDir: c.stateDir,
		Info: worker.Info(c.pool(), nil),
	})
	if err != nil {
		fmt.Fprintf(stderr, "devworker: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Joined as worker %s. Identity saved in %s.\n", id.WorkerID, c.stateDir)
	return 0
}

func runJobs(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil)).With("service", "devworker")
	log.Warn("jobs run on this machine WITHOUT isolation; use only with code you trust")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := worker.Run(ctx, worker.Config{
		ControllerURL:   c.controller,
		StateDir:        c.stateDir,
		Hypervisor:      &workertest.Hypervisor{Agent: true},
		Pool:            c.pool(),
		AllowLocalRepos: true,
		Logger:          log,
	})
	if err != nil && ctx.Err() == nil {
		log.Error("worker stopped", "err", err)
		return 1
	}
	return 0
}
