package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"connectrpc.com/connect"

	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/pkg/worker"
)

// controllerConfig is how the CLI reaches the controller's TriggerService.
// The token only ever comes from the environment, so it doesn't show up in
// process listings or shell history.
type controllerConfig struct {
	url, fingerprint, repo string
}

func (c *controllerConfig) fromEnv() {
	c.url = os.Getenv("OPIFICA_CONTROLLER")
	c.fingerprint = os.Getenv("OPIFICA_CA_FINGERPRINT")
	c.repo = os.Getenv("OPIFICA_REPO")
}

func (c *controllerConfig) client() (opificav1connect.TriggerServiceClient, error) {
	token := os.Getenv("OPIFICA_TRIGGER_TOKEN")
	switch {
	case c.url == "":
		return nil, errors.New("the controller URL is required (-controller or OPIFICA_CONTROLLER)")
	case c.fingerprint == "":
		return nil, errors.New("the controller's CA fingerprint is required (-ca-fingerprint or OPIFICA_CA_FINGERPRINT)")
	case c.repo == "":
		return nil, errors.New("the repository is required (-repo or OPIFICA_REPO)")
	case token == "":
		return nil, errors.New("OPIFICA_TRIGGER_TOKEN must be set (create one with: controller repo token create)")
	}
	u, err := url.Parse(c.url)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" {
		return nil, fmt.Errorf("controller URL must be https://host[:port], got %q", c.url)
	}
	tlsCfg, err := worker.PinnedCAConfig(c.fingerprint, u.Hostname())
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Timeout:   time.Minute,
		Transport: &http.Transport{TLSClientConfig: tlsCfg, ForceAttemptHTTP2: true},
	}
	auth := connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	})
	return opificav1connect.NewTriggerServiceClient(httpClient, c.url, connect.WithInterceptors(auth)), nil
}
