// Package repos is the controller's registry of repositories: where their
// code lives, which pool each kind of event runs in, and the tokens that may
// trigger runs.
//
// Runs are triggered the same way wherever a repository is hosted: by the
// git hook Opifica installs in hosted repositories, or by `opifica trigger` in
// a GitHub Actions workflow. Either way the controller reads the pipeline
// file at the commit itself, and decides the pool from the repository's
// routing rules, never from the caller or the pipeline.
package repos

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	gopath "path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/secrets"
	"github.com/opifica/opifica/internal/controller/store"
	"github.com/opifica/opifica/internal/controller/store/db"
	"github.com/opifica/opifica/internal/gitmirror"
	"github.com/opifica/opifica/internal/pipeline"
)

// Route sends matching events to a pool.
type Route struct {
	// Event is "push", "tag", "pull_request" or "*".
	Event string `json:"event"`
	// Ref is a glob on the branch name (push), tag name (tag) or target
	// branch (pull_request). Empty matches any.
	Ref  string `json:"ref,omitempty"`
	Pool string `json:"pool"`
}

var (
	nameRe = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$`)
	poolRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	shaRe  = regexp.MustCompile(`^([0-9a-f]{40}|[0-9a-f]{64})$`)
	// "push:main=trusted", "tag:v*=release", "pull_request=untrusted", "*=untrusted"
	routeRe = regexp.MustCompile(`^(push|tag|pull_request|\*)(?::([^=]+))?=([a-z][a-z0-9-]*)$`)
)

// DefaultRoutes send every event to the least trusted pool. Admins add rules
// for what deserves more trust.
var DefaultRoutes = []Route{{Event: "*", Pool: "untrusted"}}

// ParseRoute parses "event[:ref]=pool", e.g. "push:main=trusted".
func ParseRoute(s string) (Route, error) {
	m := routeRe.FindStringSubmatch(s)
	if m == nil {
		return Route{}, fmt.Errorf("invalid route %q: want event[:ref]=pool, e.g. push:main=trusted, tag:v*=release, pull_request=untrusted or *=untrusted", s)
	}
	r := Route{Event: m[1], Ref: m[2], Pool: m[3]}
	return r, r.check()
}

func (r Route) check() error {
	switch r.Event {
	case "push", "tag", "pull_request", "*":
	default:
		return fmt.Errorf("route event must be push, tag, pull_request or *, not %q", r.Event)
	}
	if !poolRe.MatchString(r.Pool) {
		return fmt.Errorf("invalid pool %q", r.Pool)
	}
	if _, err := gopath.Match(r.Ref, ""); err != nil {
		return fmt.Errorf("invalid ref pattern %q: %v", r.Ref, err)
	}
	return nil
}

func (r Route) String() string {
	s := r.Event
	if r.Ref != "" {
		s += ":" + r.Ref
	}
	return s + "=" + r.Pool
}

// Pool returns the pool for an event: the first matching route's.
func Pool(routes []Route, ev pipeline.Event) (string, bool) {
	name := ev.Branch
	switch ev.Type {
	case pipeline.EventTag:
		name = ev.Tag
	case pipeline.EventPullRequest:
		name = ev.BaseBranch
	}
	for _, r := range routes {
		if r.Event != "*" && r.Event != string(ev.Type) {
			continue
		}
		if r.Ref != "" {
			if ok, _ := matchRef(r.Ref, name); !ok {
				continue
			}
		}
		return r.Pool, true
	}
	return "", false
}

func matchRef(pattern, name string) (bool, error) {
	if prefix, ok := strings.CutSuffix(pattern, "/**"); ok {
		return name == prefix || strings.HasPrefix(name, prefix+"/"), nil
	}
	return gopath.Match(pattern, name)
}

// Service manages repositories and turns triggers into runs.
type Service struct {
	Store     *store.Store
	Key       *secrets.Key
	Scheduler *scheduler.Scheduler
	// Mirrors is where the controller fetches repositories to read their
	// pipeline files.
	Mirrors *gitmirror.Mirrors
}

// Repository is a registered repository.
type Repository struct {
	ID            uuid.UUID
	Name          string
	CloneURL      string
	Routes        []Route
	HasCredential bool
}

func fromDB(r db.Repository) (Repository, error) {
	var routes []Route
	if err := json.Unmarshal(r.Routes, &routes); err != nil {
		return Repository{}, fmt.Errorf("repository %s: stored routes: %w", r.Name, err)
	}
	return Repository{ID: r.ID, Name: r.Name, CloneURL: r.CloneUrl, Routes: routes, HasCredential: r.CredentialCiphertext != nil}, nil
}

func credentialPurpose(name string) string { return "repository-credential:" + name }

// Create registers a repository. A nil credential means the clone URL needs
// none. Routes default to DefaultRoutes.
func (s *Service) Create(ctx context.Context, name, cloneURL string, routes []Route, credential []byte) (Repository, error) {
	if !nameRe.MatchString(name) {
		return Repository{}, fmt.Errorf("invalid repository name %q: want owner/name", name)
	}
	if cloneURL == "" {
		return Repository{}, errors.New("a clone URL is required")
	}
	if len(routes) == 0 {
		routes = DefaultRoutes
	}
	for _, r := range routes {
		if err := r.check(); err != nil {
			return Repository{}, err
		}
	}
	routesJSON, err := json.Marshal(routes)
	if err != nil {
		return Repository{}, err
	}
	var ct, nonce []byte
	if credential != nil {
		if ct, nonce, err = s.Key.Seal(credentialPurpose(name), credential); err != nil {
			return Repository{}, err
		}
	}
	r, err := s.Store.CreateRepository(ctx, db.CreateRepositoryParams{
		ID: uuid.New(), Name: name, CloneUrl: cloneURL, Routes: routesJSON, CredentialCiphertext: ct, CredentialNonce: nonce,
	})
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") {
			return Repository{}, fmt.Errorf("repository %s already exists", name)
		}
		return Repository{}, err
	}
	return fromDB(r)
}

// Get returns a repository by name.
func (s *Service) Get(ctx context.Context, name string) (Repository, error) {
	r, err := s.Store.GetRepositoryByName(ctx, name)
	if store.IsNotFound(err) {
		return Repository{}, fmt.Errorf("no repository named %s", name)
	}
	if err != nil {
		return Repository{}, err
	}
	return fromDB(r)
}

// List returns every repository.
func (s *Service) List(ctx context.Context) ([]Repository, error) {
	rows, err := s.Store.ListRepositories(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]Repository, 0, len(rows))
	for _, r := range rows {
		repo, err := fromDB(r)
		if err != nil {
			return nil, err
		}
		out = append(out, repo)
	}
	return out, nil
}

const tokenPrefix = "opr_"

// CreateToken creates a trigger token for a repository. The token is shown
// once; only its hash is stored.
func (s *Service) CreateToken(ctx context.Context, repo, description string) (string, error) {
	r, err := s.Get(ctx, repo)
	if err != nil {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	token := tokenPrefix + base64.RawURLEncoding.EncodeToString(raw)
	if _, err := s.Store.CreateTriggerToken(ctx, db.CreateTriggerTokenParams{
		ID: uuid.New(), RepositoryID: r.ID, TokenHash: hash(token), Description: description,
	}); err != nil {
		return "", err
	}
	return token, nil
}

func hash(token string) []byte {
	h := sha256.Sum256([]byte(token))
	return h[:]
}

// ErrBadToken is returned for unknown or revoked trigger tokens.
var ErrBadToken = errors.New("invalid trigger token")

// Authenticate returns the repository a trigger token belongs to.
func (s *Service) Authenticate(ctx context.Context, token string) (Repository, error) {
	if !strings.HasPrefix(token, tokenPrefix) {
		return Repository{}, ErrBadToken
	}
	t, err := s.Store.UseTriggerToken(ctx, hash(token))
	if store.IsNotFound(err) {
		return Repository{}, ErrBadToken
	}
	if err != nil {
		return Repository{}, err
	}
	r, err := s.Store.GetRepository(ctx, t.RepositoryID)
	if err != nil {
		return Repository{}, err
	}
	return fromDB(r)
}

// credential returns a repository's decrypted read credential, if any.
func (s *Service) credential(ctx context.Context, repoID uuid.UUID) (*opificav1.Credential, error) {
	r, err := s.Store.GetRepository(ctx, repoID)
	if err != nil {
		return nil, err
	}
	if r.CredentialCiphertext == nil {
		return nil, nil
	}
	tok, err := s.Key.Open(credentialPurpose(r.Name), r.CredentialCiphertext, r.CredentialNonce)
	if err != nil {
		return nil, err
	}
	return &opificav1.Credential{Kind: &opificav1.Credential_Token{Token: string(tok)}}, nil
}

// RunCredential returns the read credential for a run's repository; it
// plugs into scheduler.Config.Credentials.
func (s *Service) RunCredential(ctx context.Context, runID uuid.UUID) (*opificav1.Credential, error) {
	run, err := s.Store.GetRun(ctx, runID)
	if err != nil {
		return nil, err
	}
	if run.RepositoryID == nil {
		return nil, nil
	}
	return s.credential(ctx, *run.RepositoryID)
}

// Outcome is what happened with a trigger.
type Outcome struct {
	RunID   uuid.UUID // zero if no run was started
	Pool    string
	Jobs    int
	Message string
}

// Trigger starts a run for a ref update in repo, if the routing rules and
// the pipeline say there is something to run. Problems with the request are
// errors; "nothing to do" is an Outcome with a message.
func (s *Service) Trigger(ctx context.Context, repo Repository, req *opificav1.TriggerRequest) (Outcome, error) {
	ev, err := event(req)
	if err != nil {
		return Outcome{}, err
	}
	pool, ok := Pool(repo.Routes, ev)
	if !ok {
		return Outcome{Message: "no routing rule of " + repo.Name + " matches this event; nothing to run"}, nil
	}

	cred, err := s.credential(ctx, repo.ID)
	if err != nil {
		return Outcome{}, err
	}
	mirror, err := s.Mirrors.Ensure(ctx, repo.CloneURL, cred, ev.SHA)
	if err != nil {
		return Outcome{}, fmt.Errorf("fetching %s: %w", repo.Name, err)
	}
	src, err := gitmirror.ReadFile(ctx, mirror, ev.SHA, filepath.ToSlash(pipeline.DefaultPath))
	if errors.Is(err, os.ErrNotExist) {
		return Outcome{Message: "no " + pipeline.DefaultPath + " at this commit; nothing to run"}, nil
	}
	if err != nil {
		return Outcome{}, err
	}

	run, err := s.Scheduler.CreateRun(ctx, scheduler.RunRequest{
		Repo: repo.Name, CloneURL: repo.CloneURL, Event: ev, Pool: pool, Pipeline: src, RepositoryID: &repo.ID,
	})
	switch {
	case errors.Is(err, scheduler.ErrNotTriggered):
		return Outcome{Pool: pool, Message: "the pipeline's triggers don't match this event; nothing to run"}, nil
	case errors.Is(err, scheduler.ErrNothingToRun):
		return Outcome{Pool: pool, Message: "every job's condition is false for this event; nothing to run"}, nil
	case err != nil:
		return Outcome{}, err
	}
	jobs, err := s.Store.ListRunJobs(ctx, run.ID)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{RunID: run.ID, Pool: pool, Jobs: len(jobs)}, nil
}

// event turns a trigger request into a pipeline event.
func event(req *opificav1.TriggerRequest) (pipeline.Event, error) {
	sha := req.GetCommitSha()
	if !shaRe.MatchString(sha) {
		return pipeline.Event{}, fmt.Errorf("commit_sha must be a full hex commit id, got %q", sha)
	}
	ref := req.GetRef()
	ev := pipeline.Event{SHA: sha, Ref: ref}
	switch req.GetEvent() {
	case "push":
		branch, ok := strings.CutPrefix(ref, "refs/heads/")
		if !ok || branch == "" {
			return pipeline.Event{}, fmt.Errorf("push ref must be refs/heads/<branch>, got %q", ref)
		}
		ev.Type, ev.Branch = pipeline.EventPush, branch
	case "tag":
		tag, ok := strings.CutPrefix(ref, "refs/tags/")
		if !ok || tag == "" {
			return pipeline.Event{}, fmt.Errorf("tag ref must be refs/tags/<tag>, got %q", ref)
		}
		ev.Type, ev.Tag = pipeline.EventTag, tag
	case "pull_request":
		if req.GetBaseBranch() == "" {
			return pipeline.Event{}, errors.New("pull requests need base_branch")
		}
		ev.Type, ev.BaseBranch = pipeline.EventPullRequest, req.GetBaseBranch()
		ev.Branch = strings.TrimPrefix(ref, "refs/heads/")
	default:
		return pipeline.Event{}, fmt.Errorf("event must be push, tag or pull_request, got %q", req.GetEvent())
	}
	return ev, nil
}
