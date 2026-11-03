package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
)

func TestHandler(t *testing.T) {
	srv := httptest.NewServer(newHandler(slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz status = %d", resp.StatusCode)
	}

	client := opificav1connect.NewWorkerServiceClient(srv.Client(), srv.URL)
	_, err = client.Join(context.Background(), connect.NewRequest(&opificav1.JoinRequest{JoinToken: "x"}))
	var cerr *connect.Error
	if !errors.As(err, &cerr) || cerr.Code() != connect.CodeUnimplemented {
		t.Errorf("Join: err = %v, want unimplemented", err)
	}
}
