// Command controller runs the Opifica controller: the API, Git provider
// integration, pipeline engine, scheduler and secrets broker.
//
// This is a skeleton: it serves the worker API with every method returning
// "unimplemented", so the wiring can be tested end to end.
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
	addr := flag.String("addr", ":8080", "address to listen on")
	flag.Parse()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("service", "controller")
	log.Info("starting", "version", version.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := server.Run(ctx, *addr, newHandler(log), log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func newHandler(log *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.Health)
	mux.Handle(opificav1connect.NewWorkerServiceHandler(&workerService{log: log}))
	return mux
}

// workerService implements the API workers connect to.
type workerService struct {
	opificav1connect.UnimplementedWorkerServiceHandler
	log *slog.Logger
}
