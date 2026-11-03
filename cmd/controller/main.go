// Command controller runs the Opifica controller: the API, Git provider
// integration, pipeline engine, scheduler and secrets broker.
//
// Implemented so far: the database, the built-in certificate authority, join
// tokens and Join. Every other RPC returns "unimplemented".
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/version"
)

const usage = `Usage: controller <command> [options]

Commands:
  serve          Run the controller
  token create   Create a one-time join token for a new build machine
  version        Print the version

Common options (or environment variables):
  -database-url  PostgreSQL URL (OPIFICA_DATABASE_URL)
  -data-dir      Directory for the CA key and certificate (OPIFICA_DATA_DIR, default ./data)
`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch {
	case args[0] == "serve":
		return serve(args[1:], stderr)
	case args[0] == "token" && len(args) > 1 && args[1] == "create":
		return tokenCreate(args[2:], stdout, stderr)
	case args[0] == "version":
		fmt.Fprintln(stdout, version.Version)
		return 0
	case args[0] == "-h" || args[0] == "--help" || args[0] == "help":
		fmt.Fprint(stdout, usage)
		return 0
	}
	fmt.Fprintf(stderr, "controller: unknown command %q\n\n%s", strings.Join(args, " "), usage)
	return 2
}

type common struct {
	databaseURL string
	dataDir     string
}

func (c *common) register(fs *flag.FlagSet) {
	fs.StringVar(&c.databaseURL, "database-url", os.Getenv("OPIFICA_DATABASE_URL"), "PostgreSQL URL")
	def := os.Getenv("OPIFICA_DATA_DIR")
	if def == "" {
		def = "data"
	}
	fs.StringVar(&c.dataDir, "data-dir", def, "directory for the CA key and certificate")
}

func (c *common) open(ctx context.Context) (*workers.Service, func(), error) {
	if c.databaseURL == "" {
		return nil, nil, errors.New("-database-url or OPIFICA_DATABASE_URL is required")
	}
	st, err := store.Open(ctx, c.databaseURL)
	if err != nil {
		return nil, nil, err
	}
	authority, err := ca.LoadOrCreate(c.dataDir + "/ca")
	if err != nil {
		st.Close()
		return nil, nil, fmt.Errorf("loading CA: %w", err)
	}
	return &workers.Service{Store: st, CA: authority}, st.Close, nil
}

func serve(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	addr := fs.String("addr", ":8443", "address to listen on")
	hosts := fs.String("hosts", defaultHosts(), "comma-separated names and IPs workers use to reach the controller")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	log := slog.New(slog.NewTextHandler(stderr, nil)).With("service", "controller")
	log.Info("starting", "version", version.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	svc, closeFn, err := c.open(ctx)
	if err != nil {
		log.Error("startup failed", "err", err)
		return 1
	}
	defer closeFn()
	svc.Log = log

	tlsCfg, err := tlsConfig(svc.CA, splitList(*hosts))
	if err != nil {
		log.Error("issuing server certificate", "err", err)
		return 1
	}
	log.Info("certificate authority ready", "fingerprint", svc.CA.Fingerprint())
	if err := server.RunTLS(ctx, *addr, newHandler(svc), tlsCfg, log); err != nil {
		log.Error("server stopped", "err", err)
		return 1
	}
	return 0
}

// tlsConfig issues a fresh server certificate and accepts, but doesn't yet
// require, worker client certificates: Join is called before a worker has
// one. RPCs that need a worker identity check it themselves.
func tlsConfig(authority *ca.CA, hosts []string) (*tls.Config, error) {
	certPEM, keyPEM, err := authority.IssueServerCert(hosts)
	if err != nil {
		return nil, err
	}
	// Send the CA too, so a joining worker can check it against the pinned
	// fingerprint before it has the CA bundle.
	certPEM = append(certPEM, authority.CertPEM()...)
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientCAs:    authority.Pool(),
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

func defaultHosts() string {
	hosts := []string{"localhost", "127.0.0.1", "::1"}
	if h, err := os.Hostname(); err == nil && h != "" {
		hosts = append([]string{h}, hosts...)
	}
	return strings.Join(hosts, ",")
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func newHandler(svc *workers.Service) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.Health)
	mux.Handle(opificav1connect.NewWorkerServiceHandler(&workerHandler{svc: svc}))
	return mux
}

// workerHandler implements the API workers connect to.
type workerHandler struct {
	opificav1connect.UnimplementedWorkerServiceHandler
	svc *workers.Service
}

func (h *workerHandler) Join(ctx context.Context, req *connect.Request[opificav1.JoinRequest]) (*connect.Response[opificav1.JoinResponse], error) {
	resp, err := h.svc.Join(ctx, req.Msg)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(resp), nil
}

type labelFlag map[string]string

func (l labelFlag) String() string { return "" }
func (l labelFlag) Set(s string) error {
	k, v, ok := strings.Cut(s, "=")
	if !ok || k == "" {
		return fmt.Errorf("label must be key=value, got %q", s)
	}
	l[k] = v
	return nil
}

func tokenCreate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("token create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	pool := fs.String("pool", "", "pool the new worker joins (required), e.g. untrusted, trusted or release")
	ttl := fs.Duration("ttl", workers.DefaultTokenTTL, "how long the token stays valid")
	desc := fs.String("description", "", "note shown when listing tokens")
	labels := labelFlag{}
	fs.Var(labels, "label", "label for the worker, key=value (repeatable)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *pool == "" {
		fmt.Fprintln(stderr, "controller: -pool is required")
		return 2
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	svc, closeFn, err := c.open(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	defer closeFn()

	tok, err := svc.CreateToken(ctx, workers.TokenRequest{Pool: *pool, Labels: labels, Description: *desc, TTL: *ttl})
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, `Join token for pool %q (valid until %s, single use):

  %s

CA fingerprint (the worker checks the controller against it):

  %s

The token is shown only once.
`, *pool, tok.ExpiresAt.Format(time.RFC3339), tok.Secret, tok.CAFingerprint)
	return 0
}
