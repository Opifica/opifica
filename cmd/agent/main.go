// Command agent runs inside every job VM. It serves the AgentService to the
// worker on the host: checkout, running steps, streaming output and
// collecting files. It never talks to the controller.
//
// This is a skeleton: every method returns "unimplemented". In production the
// agent listens on vsock or virtio-serial; -listen is for development.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/version"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7070", "address to listen on (development only)")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("service", "agent")
	log.Info("starting", "version", version.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.Health)
	mux.Handle(opificav1connect.NewAgentServiceHandler(&agentService{}))
	if err := server.Run(ctx, *listen, mux, log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

type agentService struct {
	opificav1connect.UnimplementedAgentServiceHandler
}
