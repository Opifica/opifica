// Command controller runs the Opifica controller: the API, Git provider
// integration, pipeline engine, scheduler and secrets broker.
//
// Implemented so far: the database, the built-in certificate authority, join
// tokens, Join, the Connect stream, the scheduler (runs, jobs, leases,
// retries, cancellation), job logs and secrets. Git providers come next.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"

	"github.com/opifica/opifica/internal/controller/api"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/logs"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/secrets"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/version"
)

const usage = `Usage: controller <command> [options]

Commands:
  serve          Run the controller
  token create   Create a one-time join token for a new build machine
  worker list    List workers
  worker revoke  Revoke a worker; it is disconnected and can't reconnect
  run create     Start a run from a pipeline file (until Git providers exist)
  run list       List recent runs
  run show       Show a run and its jobs
  run cancel     Cancel a run
  run logs       Print the output of a run's jobs
  secret set     Store a secret and who may use it (value from stdin)
  secret list    List secrets and their policies (never values)
  secret delete  Delete a secret
  secret access  Show who asked for a secret, and whether they got it
  version        Print the version

Common options (or environment variables):
  -database-url  PostgreSQL URL (OPIFICA_DATABASE_URL)
  -data-dir      Directory for the CA, the secrets key and job logs (OPIFICA_DATA_DIR, default ./data)
`

func main() {
	os.Exit(runWithInput(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithInput(args, strings.NewReader(""), stdout, stderr)
}

func runWithInput(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	switch {
	case args[0] == "serve":
		return serve(args[1:], stderr)
	case args[0] == "token" && len(args) > 1 && args[1] == "create":
		return tokenCreate(args[2:], stdout, stderr)
	case args[0] == "worker" && len(args) > 1 && args[1] == "list":
		return workerList(args[2:], stdout, stderr)
	case args[0] == "worker" && len(args) > 1 && args[1] == "revoke":
		return workerRevoke(args[2:], stdout, stderr)
	case args[0] == "run" && len(args) > 1:
		return runCommand(args[1], args[2:], stdout, stderr)
	case args[0] == "secret" && len(args) > 1:
		return secretCommand(args[1], args[2:], stdin, stdout, stderr)
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

	tlsCfg, err := api.TLSConfig(svc.CA, splitList(*hosts))
	if err != nil {
		log.Error("issuing server certificate", "err", err)
		return 1
	}
	log.Info("certificate authority ready", "fingerprint", svc.CA.Fingerprint())
	key, err := secrets.LoadOrCreateKey(c.dataDir)
	if err != nil {
		log.Error("loading secrets key", "err", err)
		return 1
	}
	sched := scheduler.New(svc.Store, svc.Sessions(), log.With("component", "scheduler"), scheduler.Config{
		Logs:    &logs.Store{Dir: filepath.Join(c.dataDir, "logs")},
		Secrets: &secrets.Manager{Store: svc.Store, Key: key},
	})
	svc.Messages = sched
	go sched.Run(ctx)
	if err := server.RunTLS(ctx, *addr, api.NewHandler(svc), tlsCfg, log); err != nil {
		log.Error("server stopped", "err", err)
		return 1
	}
	return 0
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

func workerList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	if err := fs.Parse(args); err != nil {
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
	ws, err := svc.Store.ListWorkers(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tHOSTNAME\tOS\tARCH\tPOOL\tJOBS\tLAST SEEN\tSTATUS")
	for _, w := range ws {
		seen, status := "never", "active"
		if w.LastSeenAt != nil {
			seen = time.Since(*w.LastSeenAt).Round(time.Second).String() + " ago"
		}
		if w.RevokedAt != nil {
			status = "revoked"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", w.ID, w.Hostname, w.Os, w.Arch, w.Pool, w.MaxConcurrentJobs, seen, status)
	}
	tw.Flush()
	return 0
}

func workerRevoke(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("worker revoke", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	fs.Usage = func() { fmt.Fprintln(stderr, "Usage: controller worker revoke [options] <worker-id>") }
	pos, err := parseWithArgs(fs, args)
	if err != nil {
		return 2
	}
	if len(pos) != 1 {
		fs.Usage()
		return 2
	}
	id, err := uuid.Parse(pos[0])
	if err != nil {
		fmt.Fprintf(stderr, "controller: invalid worker id %q\n", pos[0])
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
	if err := svc.Revoke(ctx, id); err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "Worker %s revoked. A connected controller disconnects it within %v.\n", id, workers.DefaultTouchInterval)
	return 0
}
