// Package server runs the HTTP servers behind Opifica's services.
package server

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// Health is a handler for GET /healthz.
func Health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte("ok\n"))
}

// Run serves h on addr over HTTP/1.1 and unencrypted HTTP/2 (needed for
// streaming RPCs) until ctx is cancelled, then shuts down gracefully. Use it
// only where the channel is already private, e.g. the agent inside a VM.
func Run(ctx context.Context, addr string, h http.Handler, log *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return Serve(ctx, ln, h, log)
}

// Serve is Run on an existing listener.
func Serve(ctx context.Context, ln net.Listener, h http.Handler, log *slog.Logger) error {
	return serve(ctx, ln, h, nil, log)
}

// RunTLS serves h on addr over TLS (HTTP/1.1 and HTTP/2) until ctx is
// cancelled.
func RunTLS(ctx context.Context, addr string, h http.Handler, cfg *tls.Config, log *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return ServeTLS(ctx, ln, h, cfg, log)
}

// ServeTLS is RunTLS on an existing listener.
func ServeTLS(ctx context.Context, ln net.Listener, h http.Handler, cfg *tls.Config, log *slog.Logger) error {
	return serve(ctx, ln, h, cfg, log)
}

func serve(ctx context.Context, ln net.Listener, h http.Handler, tlsCfg *tls.Config, log *slog.Logger) error {
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	if tlsCfg != nil {
		protocols.SetHTTP2(true)
	} else {
		protocols.SetUnencryptedHTTP2(true)
	}
	srv := &http.Server{
		Handler:           h,
		Protocols:         protocols,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second,
		// TLS handshake failures from bad clients are routine; keep them out
		// of the default logger.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelDebug),
	}

	errc := make(chan error, 1)
	go func() {
		if tlsCfg != nil {
			errc <- srv.ServeTLS(ln, "", "")
		} else {
			errc <- srv.Serve(ln)
		}
	}()
	log.Info("listening", "addr", ln.Addr().String(), "tls", tlsCfg != nil)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
