package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/opifica/opifica/internal/controller/repos"
	"github.com/opifica/opifica/internal/controller/scheduler"
	"github.com/opifica/opifica/internal/controller/secrets"
	"github.com/opifica/opifica/internal/gitmirror"
)

func repoCommand(sub string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	switch sub {
	case "create":
		return repoCreate(args, stdin, stdout, stderr)
	case "list":
		return repoList(args, stdout, stderr)
	case "delete":
		return repoDelete(args, stdout, stderr)
	case "token":
		if len(args) > 0 && args[0] == "create" {
			return repoTokenCreate(args[1:], stdout, stderr)
		}
	}
	fmt.Fprintf(stderr, "controller: unknown command \"repo %s\"\n\n%s", sub, usage)
	return 2
}

func withRepos(c common, stderr io.Writer, fn func(ctx context.Context, r *repos.Service) error) int {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	svc, closeFn, err := c.open(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	defer closeFn()
	key, err := secrets.LoadOrCreateKey(c.dataDir)
	if err != nil {
		fmt.Fprintf(stderr, "controller: loading secrets key: %v\n", err)
		return 1
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	r := &repos.Service{
		Store: svc.Store, Key: key,
		Scheduler: scheduler.New(svc.Store, svc.Sessions(), quiet, scheduler.Config{}),
		Mirrors:   &gitmirror.Mirrors{Dir: filepath.Join(c.dataDir, "mirrors")},
	}
	if err := fn(ctx, r); err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	return 0
}

func repoCreate(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	cloneURL := fs.String("clone-url", "", "where the controller and workers fetch the code (required)")
	var routes listFlag
	fs.Var(&routes, "route", "event[:ref]=pool, first match wins (repeatable; default *=untrusted), e.g. push:main=trusted")
	credStdin := fs.Bool("credential-stdin", false, "read a read-only clone token from standard input")
	fs.Usage = func() {
		fmt.Fprint(stderr, `Usage: controller repo create [options] OWNER/NAME

Registers a repository and prints its first trigger token.

Routes decide which pool runs each event; the first matching route wins and
an event no route matches starts no run. For example:

  -route push:main=trusted -route tag:v*=release -route '*=untrusted'

Options:
`)
		fs.PrintDefaults()
	}
	pos, err := parseWithArgs(fs, args)
	if err != nil {
		return 2
	}
	if len(pos) != 1 || *cloneURL == "" {
		fs.Usage()
		return 2
	}
	var parsed []repos.Route
	for _, r := range routes {
		route, err := repos.ParseRoute(r)
		if err != nil {
			fmt.Fprintf(stderr, "controller: %v\n", err)
			return 2
		}
		parsed = append(parsed, route)
	}
	var cred []byte
	if *credStdin {
		b, err := io.ReadAll(io.LimitReader(stdin, 16<<10))
		if err != nil || len(bytes.TrimSpace(b)) == 0 {
			fmt.Fprintln(stderr, "controller: -credential-stdin: no token on standard input")
			return 2
		}
		cred = bytes.TrimSpace(b)
	}
	return withRepos(c, stderr, func(ctx context.Context, r *repos.Service) error {
		repo, err := r.Create(ctx, pos[0], *cloneURL, parsed, cred)
		if err != nil {
			return err
		}
		token, err := r.CreateToken(ctx, repo.Name, "created with the repository")
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Repository %s registered.\n\nRoutes (first match wins):\n", repo.Name)
		for _, rt := range repo.Routes {
			fmt.Fprintf(stdout, "  %s\n", rt)
		}
		fmt.Fprintf(stdout, "\nTrigger token (shown once; store it as OPIFICA_TRIGGER_TOKEN):\n\n  %s\n", token)
		return nil
	})
}

func repoList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return withRepos(c, stderr, func(ctx context.Context, r *repos.Service) error {
		list, err := r.List(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tCLONE URL\tCREDENTIAL\tROUTES")
		for _, repo := range list {
			var rs []string
			for _, rt := range repo.Routes {
				rs = append(rs, rt.String())
			}
			cred := "no"
			if repo.HasCredential {
				cred = "yes"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", repo.Name, repo.CloneURL, cred, strings.Join(rs, " "))
		}
		return tw.Flush()
	})
}

func repoDelete(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	pos, err := parseWithArgs(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(stderr, "Usage: controller repo delete [options] OWNER/NAME")
		return 2
	}
	return withRepos(c, stderr, func(ctx context.Context, r *repos.Service) error {
		n, err := r.Store.DeleteRepository(ctx, pos[0])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("no repository named %s", pos[0])
		}
		fmt.Fprintf(stdout, "Repository %s deleted; its trigger tokens no longer work. Past runs are kept.\n", pos[0])
		return nil
	})
}

func repoTokenCreate(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("repo token create", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	desc := fs.String("description", "", "where the token is used, e.g. github-actions")
	pos, err := parseWithArgs(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(stderr, "Usage: controller repo token create [options] OWNER/NAME")
		return 2
	}
	return withRepos(c, stderr, func(ctx context.Context, r *repos.Service) error {
		token, err := r.CreateToken(ctx, pos[0], *desc)
		if err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Trigger token for %s (shown once):\n\n  %s\n", pos[0], token)
		return nil
	})
}
