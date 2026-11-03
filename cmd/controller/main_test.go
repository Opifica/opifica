package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/api"
	"github.com/opifica/opifica/internal/server"
	"github.com/opifica/opifica/internal/testutil/pgtest"
	"github.com/opifica/opifica/pkg/worker"
)

func TestMain(m *testing.M) { os.Exit(pgtest.Run(m)) }

// End to end through the binary's own code paths: create a token with
// "token create", serve over TLS, and join as a worker that pins the CA.
func TestTokenCreateAndJoinOverTLS(t *testing.T) {
	dbURL := pgtest.New(t)
	dataDir := t.TempDir()

	var out, errOut bytes.Buffer
	code := run([]string{"token", "create", "-database-url", dbURL, "-data-dir", dataDir,
		"-pool", "trusted", "-label", "region=eu"}, &out, &errOut)
	if code != 0 {
		t.Fatalf("token create exited %d: %s", code, errOut.String())
	}
	token := regexp.MustCompile(`opj_\S+`).FindString(out.String())
	fingerprint := regexp.MustCompile(`sha256:[0-9a-f]{64}`).FindString(out.String())
	if token == "" || fingerprint == "" {
		t.Fatalf("token create output lacks token or fingerprint:\n%s", out.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := common{databaseURL: dbURL, dataDir: dataDir}
	svc, closeFn, err := c.open(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	svc.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	tlsCfg, err := api.TLSConfig(svc.CA, []string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go server.ServeTLS(ctx, ln, api.NewHandler(svc), tlsCfg, svc.Log)

	// The worker has no CA yet: it accepts the controller only if the
	// certificate chains to a CA with the pinned fingerprint.
	pinned, err := worker.PinnedCAConfig(fingerprint, "127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: &http.Transport{ForceAttemptHTTP2: true, TLSClientConfig: pinned}}
	base := "https://" + ln.Addr().String()
	waitHealthy(t, client, base)

	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	der, _ := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	wc := opificav1connect.NewWorkerServiceClient(client, base)
	resp, err := wc.Join(ctx, connect.NewRequest(&opificav1.JoinRequest{
		JoinToken: token,
		CsrPem:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}),
		Info: &opificav1.WorkerInfo{
			Hostname: "build-1", Os: opificav1.OperatingSystem_OPERATING_SYSTEM_LINUX,
			Arch: opificav1.Architecture_ARCHITECTURE_AMD64, MaxConcurrentJobs: 4,
		},
	}))
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if resp.Msg.WorkerId == "" || len(resp.Msg.CertificatePem) == 0 {
		t.Errorf("Join response = %+v", resp.Msg)
	}

	// Connect without a client certificate is refused.
	stream := wc.Connect(ctx)
	stream.Send(&opificav1.ConnectRequest{})
	if _, err := stream.Receive(); connect.CodeOf(err) != connect.CodeUnauthenticated {
		t.Errorf("Connect without a certificate: err = %v, want unauthenticated", err)
	}
	stream.CloseRequest()
	stream.CloseResponse()

	// The CA survived on disk: a second start must not create a new one.
	if _, err := os.Stat(filepath.Join(dataDir, "ca", "ca.key")); err != nil {
		t.Errorf("CA key not persisted: %v", err)
	}
}

func waitHealthy(t *testing.T, c *http.Client, base string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := c.Get(base + "/healthz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("controller not healthy: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestUsage(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(nil, &out, &errOut); code != 2 {
		t.Errorf("no args: exit %d", code)
	}
	if code := run([]string{"bogus"}, &out, &errOut); code != 2 {
		t.Errorf("unknown command: exit %d", code)
	}
	if code := run([]string{"token", "create", "-database-url", "x"}, &out, &errOut); code != 2 {
		t.Errorf("token create without -pool: exit %d", code)
	}
}

func TestRunCommands(t *testing.T) {
	dbURL := pgtest.New(t)
	dir := t.TempDir()
	file := filepath.Join(dir, "pipeline.yaml")
	os.WriteFile(file, []byte("on: push\njobs:\n  build:\n    runs-on: linux\n    steps: [{ run: make }]\n  deploy:\n    needs: build\n    runs-on: linux\n    steps: [{ run: make deploy }]\n"), 0o644)
	common := []string{"-database-url", dbURL, "-data-dir", dir}

	var out, errOut bytes.Buffer
	if code := run(append([]string{"run", "create", "-repo", "acme/app", "-pool", "trusted", "-sha", "abc123", "-file", file}, common...), &out, &errOut); code != 0 {
		t.Fatalf("run create: exit %d: %s", code, errOut.String())
	}
	id := regexp.MustCompile(`Run (\S+) created with 2 job\(s\)`).FindStringSubmatch(out.String())
	if id == nil {
		t.Fatalf("run create output: %s", out.String())
	}

	out.Reset()
	if code := run(append([]string{"run", "list"}, common...), &out, &errOut); code != 0 || !regexp.MustCompile(`acme/app\s+push\s+main\s+abc123\s+trusted\s+running`).MatchString(out.String()) {
		t.Errorf("run list (exit %d):\n%s", code, out.String())
	}
	out.Reset()
	if code := run(append([]string{"run", "show", id[1]}, common...), &out, &errOut); code != 0 ||
		!regexp.MustCompile(`build\s+linux\s+queued`).MatchString(out.String()) || !regexp.MustCompile(`deploy\s+linux\s+pending`).MatchString(out.String()) {
		t.Errorf("run show (exit %d):\n%s", code, out.String())
	}
	out.Reset()
	if code := run(append([]string{"run", "cancel", id[1]}, common...), &out, &errOut); code != 0 {
		t.Errorf("run cancel: exit %d: %s", code, errOut.String())
	}
	out.Reset()
	run(append([]string{"run", "show", id[1]}, common...), &out, &errOut)
	if !regexp.MustCompile(`: cancelled`).MatchString(out.String()) {
		t.Errorf("after cancel:\n%s", out.String())
	}
}
