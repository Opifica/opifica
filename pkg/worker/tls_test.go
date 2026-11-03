package worker_test

import (
	"crypto/tls"
	"io"
	"strings"
	"testing"

	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/pkg/worker"
)

// serve starts a TLS server with a certificate from authority for hosts,
// sending the CA in the chain like the controller does.
func serve(t *testing.T, authority *ca.CA, hosts []string, sendCA bool) string {
	t.Helper()
	certPEM, keyPEM, err := authority.IssueServerCert(hosts)
	if err != nil {
		t.Fatal(err)
	}
	if sendCA {
		certPEM = append(certPEM, authority.CertPEM()...)
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{pair}, MinVersion: tls.VersionTLS13})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c.(*tls.Conn).Handshake()
				io.Copy(io.Discard, c)
				c.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func dial(t *testing.T, addr, fingerprint, host string) error {
	t.Helper()
	cfg, err := worker.PinnedCAConfig(fingerprint, host)
	if err != nil {
		return err
	}
	c, err := tls.Dial("tcp", addr, cfg)
	if err == nil {
		c.Close()
	}
	return err
}

func TestPinnedCA(t *testing.T) {
	good, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	impostor, err := ca.LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	addr := serve(t, good, []string{"127.0.0.1", "controller.local"}, true)
	if err := dial(t, addr, good.Fingerprint(), "127.0.0.1"); err != nil {
		t.Errorf("genuine controller rejected: %v", err)
	}

	tests := []struct {
		name, addr, fingerprint, host, want string
	}{
		{"wrong pin", addr, impostor.Fingerprint(), "127.0.0.1", "does not match the pinned fingerprint"},
		{"wrong host", addr, good.Fingerprint(), "evil.example", "controller certificate"},
		{"impostor controller", serve(t, impostor, []string{"127.0.0.1"}, true), good.Fingerprint(), "127.0.0.1", "does not match"},
		{"no CA in chain", serve(t, good, []string{"127.0.0.1"}, false), good.Fingerprint(), "127.0.0.1", "did not send its CA"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dial(t, tt.addr, tt.fingerprint, tt.host)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want %q", err, tt.want)
			}
		})
	}

	if _, err := worker.PinnedCAConfig("md5:abc", "x"); err == nil {
		t.Error("malformed fingerprint accepted")
	}
}
