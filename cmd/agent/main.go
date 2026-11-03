// Command agent runs inside every job VM. It serves the AgentService to the
// worker on the host: checkout, running steps, streaming output and
// collecting files. It never talks to the controller.
//
// In a VM the agent listens on vsock (not implemented yet; see the worker
// repositories). -listen with a TCP address or unix:/path is for development.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/opifica/opifica/internal/agent"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/version"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7070", "TCP address or unix:/path/to/socket (development)")
	root := flag.String("root", defaultRoot(), "work directory; every request path stays inside it")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("service", "agent")
	log.Info("starting", "version", version.Version, "root", *root)

	if err := os.MkdirAll(*root, 0o755); err != nil {
		log.Error("creating work directory", "err", err)
		os.Exit(1)
	}
	network, addr := "tcp", *listen
	if p, ok := strings.CutPrefix(*listen, "unix:"); ok {
		network, addr = "unix", p
		os.Remove(p)
	}
	ln, err := net.Listen(network, addr)
	if err != nil {
		log.Error("listening", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Serve(ctx, ln, agent.Handler(&agent.Server{Root: *root, Log: log}), log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func defaultRoot() string {
	if d, err := os.UserHomeDir(); err == nil {
		return filepath.Join(d, "opifica-work")
	}
	return "opifica-work"
}
