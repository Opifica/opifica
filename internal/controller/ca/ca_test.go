package ca

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func newCSR(t *testing.T, key any, dnsNames ...string) []byte {
	t.Helper()
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject:  pkix.Name{CommonName: "i-am-the-controller"},
		DNSNames: dnsNames,
	}, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
}

func ecKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func parseCert(t *testing.T, p []byte) *x509.Certificate {
	t.Helper()
	b, _ := pem.Decode(p)
	if b == nil {
		t.Fatal("no PEM block")
	}
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestLoadOrCreatePersists(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ca")
	c1, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	c2, err := LoadOrCreate(dir)
	if err != nil {
		t.Fatal(err)
	}
	if c1.Fingerprint() != c2.Fingerprint() {
		t.Error("reloading created a different CA")
	}
	if !strings.HasPrefix(c1.Fingerprint(), "sha256:") || len(c1.Fingerprint()) != 7+64 {
		t.Errorf("fingerprint = %q", c1.Fingerprint())
	}
	info, err := os.Stat(filepath.Join(dir, keyFile))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("CA key permissions = %v, want 0600", info.Mode().Perm())
	}
}

func TestLoadOrCreateRejectsBrokenState(t *testing.T) {
	dir := t.TempDir()
	if _, err := LoadOrCreate(dir); err != nil {
		t.Fatal(err)
	}

	// Only one of the two files.
	half := t.TempDir()
	data, _ := os.ReadFile(filepath.Join(dir, certFile))
	os.WriteFile(filepath.Join(half, certFile), data, 0o644)
	if _, err := LoadOrCreate(half); err == nil || !strings.Contains(err.Error(), "only one of") {
		t.Errorf("half-present CA: err = %v", err)
	}

	// Key from a different CA.
	other := t.TempDir()
	if _, err := LoadOrCreate(other); err != nil {
		t.Fatal(err)
	}
	key, _ := os.ReadFile(filepath.Join(other, keyFile))
	os.WriteFile(filepath.Join(dir, keyFile), key, 0o600)
	if _, err := LoadOrCreate(dir); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Errorf("mismatched key: err = %v", err)
	}
}

func TestSignWorkerCSR(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// The CSR asks to be "the controller"; it must get only the worker identity.
	certPEM, serial, err := c.SignWorkerCSR(newCSR(t, ecKey(t), "controller.example"), "w-123")
	if err != nil {
		t.Fatal(err)
	}
	cert := parseCert(t, certPEM)
	if cert.SerialNumber.Text(16) != serial {
		t.Errorf("serial = %s, returned %s", cert.SerialNumber.Text(16), serial)
	}
	if len(cert.DNSNames) != 0 || cert.Subject.CommonName != "w-123" {
		t.Errorf("certificate took names from the CSR: CN=%q DNS=%v", cert.Subject.CommonName, cert.DNSNames)
	}
	if id, err := WorkerID(cert); err != nil || id != "w-123" {
		t.Errorf("WorkerID = %q, %v", id, err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots: c.Pool(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Errorf("worker certificate doesn't verify as a client certificate: %v", err)
	}
	if _, err := cert.Verify(x509.VerifyOptions{
		Roots: c.Pool(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}); err == nil {
		t.Error("worker certificate is accepted as a server certificate")
	}
}

func TestSignWorkerCSRRejects(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	weak, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	tampered := newCSR(t, ecKey(t))
	b, _ := pem.Decode(tampered)
	b.Bytes[len(b.Bytes)-5] ^= 0xff // corrupt the signature
	tampered = pem.EncodeToMemory(b)

	for name, csr := range map[string][]byte{
		"not PEM":       []byte("hello"),
		"wrong type":    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{1}}),
		"bad signature": tampered,
		"weak key":      newCSR(t, weak),
	} {
		if _, _, err := c.SignWorkerCSR(csr, "w"); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// A full mTLS handshake: the worker verifies the controller with the CA
// bundle, and the controller verifies the worker's client certificate.
func TestMutualTLS(t *testing.T) {
	c, err := LoadOrCreate(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srvCert, srvKey, err := c.IssueServerCert([]string{"controller.local", "127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	serverPair, err := tls.X509KeyPair(srvCert, srvKey)
	if err != nil {
		t.Fatal(err)
	}

	wkey := ecKey(t)
	wcert, _, err := c.SignWorkerCSR(newCSR(t, wkey), "w-42")
	if err != nil {
		t.Fatal(err)
	}
	wkeyDER, _ := x509.MarshalPKCS8PrivateKey(wkey)
	workerPair, err := tls.X509KeyPair(wcert, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: wkeyDER}))
	if err != nil {
		t.Fatal(err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{serverPair},
		ClientCAs:    c.Pool(),
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	gotID := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			gotID <- "accept: " + err.Error()
			return
		}
		defer conn.Close()
		tc := conn.(*tls.Conn)
		if err := tc.Handshake(); err != nil {
			gotID <- "handshake: " + err.Error()
			return
		}
		id, err := WorkerID(tc.ConnectionState().PeerCertificates[0])
		if err != nil {
			gotID <- err.Error()
			return
		}
		gotID <- id
		io.Copy(io.Discard, conn)
	}()

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	conn, err := tls.Dial("tcp", "127.0.0.1:"+port, &tls.Config{
		Certificates: []tls.Certificate{workerPair},
		RootCAs:      c.Pool(),
		MinVersion:   tls.VersionTLS13,
	})
	if err != nil {
		t.Fatalf("worker could not connect: %v", err)
	}
	conn.Close()
	if id := <-gotID; id != "w-42" {
		t.Errorf("controller saw worker %q, want w-42", id)
	}
}
