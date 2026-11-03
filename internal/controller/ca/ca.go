// Package ca is the controller's built-in certificate authority. It issues
// the client certificates workers use for mTLS, and the controller's own
// server certificate.
//
// The CA key is kept in a file readable only by the controller. Moving it to
// an HSM or KMS is planned (see docs/ROADMAP.md, M5).
package ca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const (
	certFile = "ca.crt"
	keyFile  = "ca.key"

	// WorkerCertLifetime is how long a worker certificate is valid. Workers
	// will renew before expiry (not implemented yet).
	WorkerCertLifetime = 30 * 24 * time.Hour
	// ServerCertLifetime is how long the controller's server certificate is
	// valid. It is reissued at every start.
	ServerCertLifetime = 90 * 24 * time.Hour
	caLifetime         = 10 * 365 * 24 * time.Hour
)

// CA signs certificates.
type CA struct {
	cert    *x509.Certificate
	certPEM []byte
	key     crypto.Signer
	now     func() time.Time
}

// LoadOrCreate loads the CA from dir, creating a new one on first start.
func LoadOrCreate(dir string) (*CA, error) {
	certPath, keyPath := filepath.Join(dir, certFile), filepath.Join(dir, keyFile)
	certPEM, certErr := os.ReadFile(certPath)
	keyPEM, keyErr := os.ReadFile(keyPath)
	switch {
	case certErr == nil && keyErr == nil:
		return parse(certPEM, keyPEM)
	case errors.Is(certErr, os.ErrNotExist) && errors.Is(keyErr, os.ErrNotExist):
		// First start.
	case certErr != nil && !errors.Is(certErr, os.ErrNotExist):
		return nil, certErr
	case keyErr != nil && !errors.Is(keyErr, os.ErrNotExist):
		return nil, keyErr
	default:
		return nil, fmt.Errorf("%s has only one of %s and %s; restore the missing file or remove both", dir, certFile, keyFile)
	}

	c, keyPEM, err := create()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// Write the key first and exclusively, so two controllers starting at
	// once can't end up with different CAs.
	if err := writeExclusive(keyPath, keyPEM, 0o600); err != nil {
		return nil, err
	}
	if err := writeExclusive(certPath, c.certPEM, 0o644); err != nil {
		return nil, err
	}
	return c, nil
}

func writeExclusive(path string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func create() (*CA, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := newSerial()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Opifica CA", Organization: []string{"Opifica"}},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(caLifetime),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	c, err := parse(certPEM, keyPEM)
	return c, keyPEM, err
}

func parse(certPEM, keyPEM []byte) (*CA, error) {
	cb, _ := pem.Decode(certPEM)
	if cb == nil || cb.Type != "CERTIFICATE" {
		return nil, errors.New("CA certificate: no PEM certificate found")
	}
	cert, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("CA certificate: %w", err)
	}
	kb, _ := pem.Decode(keyPEM)
	if kb == nil {
		return nil, errors.New("CA key: no PEM block found")
	}
	k, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, fmt.Errorf("CA key: %w", err)
	}
	signer, ok := k.(crypto.Signer)
	if !ok {
		return nil, errors.New("CA key cannot sign")
	}
	if !publicKeysEqual(cert.PublicKey, signer.Public()) {
		return nil, errors.New("CA key does not match the CA certificate")
	}
	return &CA{cert: cert, certPEM: certPEM, key: signer, now: time.Now}, nil
}

func publicKeysEqual(a, b crypto.PublicKey) bool {
	type equaler interface{ Equal(crypto.PublicKey) bool }
	e, ok := a.(equaler)
	return ok && e.Equal(b)
}

// CertPEM returns the CA certificate, which workers use to verify the controller.
func (c *CA) CertPEM() []byte { return c.certPEM }

// Pool returns a certificate pool containing only this CA.
func (c *CA) Pool() *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(c.cert)
	return p
}

// Fingerprint is "sha256:<hex>" of the CA's public key. A worker joining for
// the first time checks the controller against it, so a join token can't be
// used against an impostor controller.
func (c *CA) Fingerprint() string {
	sum := sha256.Sum256(c.cert.RawSubjectPublicKeyInfo)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// WorkerURI is the identity placed in a worker certificate's URI SAN.
func WorkerURI(workerID string) string { return "opifica://worker/" + workerID }

// SignWorkerCSR checks a worker's certificate request and issues a client
// certificate for workerID. Whatever names the CSR asks for are ignored: the
// identity comes only from workerID.
func (c *CA) SignWorkerCSR(csrPEM []byte, workerID string) (certPEM []byte, serial string, err error) {
	b, _ := pem.Decode(csrPEM)
	if b == nil || b.Type != "CERTIFICATE REQUEST" {
		return nil, "", errors.New("no PEM certificate request found")
	}
	csr, err := x509.ParseCertificateRequest(b.Bytes)
	if err != nil {
		return nil, "", fmt.Errorf("parsing certificate request: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, "", fmt.Errorf("certificate request signature: %w", err)
	}
	if err := checkKeyStrength(csr.PublicKey); err != nil {
		return nil, "", err
	}
	uri, err := url.Parse(WorkerURI(workerID))
	if err != nil {
		return nil, "", err
	}
	sn, err := newSerial()
	if err != nil {
		return nil, "", err
	}
	now := c.now()
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		Subject:      pkix.Name{CommonName: workerID, Organization: []string{"Opifica worker"}},
		URIs:         []*url.URL{uri},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(WorkerCertLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, csr.PublicKey, c.key)
	if err != nil {
		return nil, "", err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), sn.Text(16), nil
}

// IssueServerCert issues a server certificate for the controller with the
// given DNS names and IP addresses.
func (c *CA) IssueServerCert(hosts []string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	sn, err := newSerial()
	if err != nil {
		return nil, nil, err
	}
	now := c.now()
	tmpl := &x509.Certificate{
		SerialNumber: sn,
		Subject:      pkix.Name{CommonName: "Opifica controller"},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(ServerCertLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
		} else {
			tmpl.DNSNames = append(tmpl.DNSNames, h)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), nil
}

// WorkerID returns the worker identity in a verified client certificate.
func WorkerID(cert *x509.Certificate) (string, error) {
	for _, u := range cert.URIs {
		if u.Scheme == "opifica" && u.Host == "worker" && len(u.Path) > 1 {
			return u.Path[1:], nil
		}
	}
	return "", errors.New("certificate has no Opifica worker identity")
}

func checkKeyStrength(pub crypto.PublicKey) error {
	switch k := pub.(type) {
	case *ecdsa.PublicKey:
		if k.Curve.Params().BitSize < 256 {
			return fmt.Errorf("ECDSA key too small (%d bits)", k.Curve.Params().BitSize)
		}
		return nil
	case interface{ Size() int }: // *rsa.PublicKey
		if k.Size()*8 < 2048 {
			return fmt.Errorf("RSA key too small (%d bits)", k.Size()*8)
		}
		return nil
	}
	if _, ok := pub.(interface{ Equal(crypto.PublicKey) bool }); ok {
		return nil // ed25519 and other modern keys
	}
	return fmt.Errorf("unsupported key type %T", pub)
}

func newSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}
