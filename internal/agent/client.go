package agent

import (
	"context"
	"net"
	"net/http"

	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
)

// NewClient returns an AgentService client that reaches the agent through
// dial, e.g. a VM's vsock connection. The channel is private to the host and
// the guest, so it is plain HTTP/2 without TLS.
func NewClient(dial func(ctx context.Context) (net.Conn, error)) opificav1connect.AgentServiceClient {
	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{
		Protocols: protocols,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return dial(ctx)
		},
	}
	return opificav1connect.NewAgentServiceClient(&http.Client{Transport: transport}, "http://agent")
}

// Handler returns the agent's HTTP handler.
func Handler(s *Server) http.Handler {
	mux := http.NewServeMux()
	mux.Handle(opificav1connect.NewAgentServiceHandler(s))
	return mux
}
