package api

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/gen/opifica/v1/opificav1connect"
	"github.com/opifica/opifica/internal/controller/logs"
	"github.com/opifica/opifica/internal/controller/repos"
	"github.com/opifica/opifica/internal/controller/store"
)

const maxLogChunk = 256 << 10

// triggerHandler implements TriggerService. Every call is authorized by a
// repository's trigger token and only reaches that repository's runs.
type triggerHandler struct {
	opificav1connect.UnimplementedTriggerServiceHandler
	repos *repos.Service
	logs  *logs.Store
}

var (
	errNoToken   = connect.NewError(connect.CodeUnauthenticated, errors.New("a trigger token is required (Authorization: Bearer opr_...)"))
	errBadToken  = connect.NewError(connect.CodeUnauthenticated, repos.ErrBadToken)
	errNotYours  = connect.NewError(connect.CodePermissionDenied, errors.New("this token is for another repository"))
	errNoSuchRun = connect.NewError(connect.CodeNotFound, errors.New("no such run for this repository"))
)

func (h *triggerHandler) auth(ctx context.Context, header string) (repos.Repository, error) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		return repos.Repository{}, errNoToken
	}
	repo, err := h.repos.Authenticate(ctx, token)
	if errors.Is(err, repos.ErrBadToken) {
		return repos.Repository{}, errBadToken
	}
	if err != nil {
		return repos.Repository{}, connect.NewError(connect.CodeUnavailable, errors.New("try again later"))
	}
	return repo, nil
}

func (h *triggerHandler) Trigger(ctx context.Context, req *connect.Request[opificav1.TriggerRequest]) (*connect.Response[opificav1.TriggerResponse], error) {
	repo, err := h.auth(ctx, req.Header().Get("Authorization"))
	if err != nil {
		return nil, err
	}
	if req.Msg.GetRepository() != repo.Name {
		return nil, errNotYours
	}
	out, err := h.repos.Trigger(ctx, repo, req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	resp := &opificav1.TriggerResponse{Pool: out.Pool, Jobs: int32(out.Jobs), Message: out.Message}
	if out.RunID != uuid.Nil {
		resp.RunId = out.RunID.String()
	}
	return connect.NewResponse(resp), nil
}

func (h *triggerHandler) GetRun(ctx context.Context, req *connect.Request[opificav1.GetRunRequest]) (*connect.Response[opificav1.GetRunResponse], error) {
	repo, err := h.auth(ctx, req.Header().Get("Authorization"))
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(req.Msg.GetRunId())
	if err != nil {
		return nil, errNoSuchRun
	}
	run, err := h.repos.Store.GetRun(ctx, id)
	if err != nil || run.RepositoryID == nil || *run.RepositoryID != repo.ID {
		return nil, errNoSuchRun
	}
	jobs, err := h.repos.Store.ListRunJobs(ctx, id)
	if err != nil {
		return nil, err
	}
	resp := &opificav1.GetRunResponse{State: run.State}
	for _, j := range jobs {
		resp.Jobs = append(resp.Jobs, &opificav1.JobSummary{
			Id: j.ID.String(), Name: j.Name, Os: j.Os, State: j.State, ExitCode: j.ExitCode, Error: j.Error,
		})
	}
	return connect.NewResponse(resp), nil
}

func (h *triggerHandler) GetJobLog(ctx context.Context, req *connect.Request[opificav1.GetJobLogRequest]) (*connect.Response[opificav1.GetJobLogResponse], error) {
	repo, err := h.auth(ctx, req.Header().Get("Authorization"))
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(req.Msg.GetJobId())
	if err != nil {
		return nil, errNoSuchRun
	}
	job, err := h.repos.Store.GetJob(ctx, id)
	if err != nil {
		return nil, errNoSuchRun
	}
	run, err := h.repos.Store.GetRun(ctx, job.RunID)
	if store.IsNotFound(err) || err == nil && (run.RepositoryID == nil || *run.RepositoryID != repo.ID) {
		return nil, errNoSuchRun
	}
	if err != nil {
		return nil, err
	}
	data, next, err := h.logs.ReadFrom(id, req.Msg.GetOffset(), maxLogChunk)
	if err != nil {
		return nil, err
	}
	finished := job.State != "pending" && job.State != "queued" && job.State != "assigned" && job.State != "running"
	return connect.NewResponse(&opificav1.GetJobLogResponse{
		Data: data, NextOffset: next, Complete: finished && len(data) < maxLogChunk,
	}), nil
}
