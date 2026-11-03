// Package api assembles the controller's HTTP handler and TLS settings.
package api

import (
	"context"
	"crypto/tls"
	"net/http"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/ca"
	"github.com/opifica/opifica/internal/controller/logs"
	"github.com/opifica/opifica/internal/controller/repos"
	"github.com/opifica/opifica/internal/controller/workers"
	"github.com/opifica/opifica/internal/server"
)

// Deps are the services behind the API.
type Deps struct {
	Workers *workers.Service
	// Repos and Logs serve the TriggerService; without Repos it is off.
	Repos *repos.Service
	Logs  *logs.Store
}

// NewHandler returns the controller's HTTP handler.
func NewHandler(d Deps) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", server.Health)
	mux.Handle(opificav1connect.NewWorkerServiceHandler(&workerHandler{svc: d.Workers}))
	if d.Repos != nil {
		mux.Handle(opificav1connect.NewTriggerServiceHandler(&triggerHandler{repos: d.Repos, logs: d.Logs}))
	}
	return workers.WithPeerCert(mux)
}

// TLSConfig issues a fresh server certificate for hosts. Worker client
// certificates are verified when presented but not required at the TLS
// layer, because Join is called before a worker has one; Connect requires
// one itself.
func TLSConfig(authority *ca.CA, hosts []string) (*tls.Config, error) {
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

func (h *workerHandler) Connect(ctx context.Context, stream *connect.BidiStream[opificav1.ConnectRequest, opificav1.ConnectResponse]) error {
	return h.svc.Connect(ctx, workers.PeerCert(ctx), stream)
}
