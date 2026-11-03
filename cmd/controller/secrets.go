package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/opifica/opifica/internal/controller/secrets"
	"github.com/opifica/opifica/internal/controller/store/db"
)

type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

func secretCommand(sub string, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	switch sub {
	case "set":
		return secretSet(args, stdin, stdout, stderr)
	case "list":
		return secretList(args, stdout, stderr)
	case "delete":
		return secretDelete(args, stdout, stderr)
	case "access":
		return secretAccess(args, stdout, stderr)
	}
	fmt.Fprintf(stderr, "controller: unknown command \"secret %s\"\n\n%s", sub, usage)
	return 2
}

func withSecrets(c common, stderr io.Writer, fn func(ctx context.Context, m *secrets.Manager) error) int {
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
	if err := fn(ctx, &secrets.Manager{Store: svc.Store, Key: key}); err != nil {
		fmt.Fprintf(stderr, "controller: %v\n", err)
		return 1
	}
	return 0
}

func secretSet(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("secret set", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	var repos, pools, refs listFlag
	fs.Var(&repos, "repo", "repository glob that may use it, e.g. acme/* or * (repeatable, required)")
	fs.Var(&pools, "pool", "pool that may use it (repeatable, required)")
	fs.Var(&refs, "ref", "ref glob that may use it, e.g. refs/heads/main or refs/tags/v* (repeatable; default any)")
	allowPR := fs.Bool("allow-pull-requests", false, "let pull requests use it (they run unreviewed code)")
	desc := fs.String("description", "", "note shown in secret list")
	fs.Usage = func() {
		fmt.Fprintln(stderr, "Usage: controller secret set [options] NAME < value\n\nThe value is read from standard input, never from the command line.\n\nOptions:")
		fs.PrintDefaults()
	}
	pos, err := parseWithArgs(fs, args)
	if err != nil {
		return 2
	}
	if len(pos) != 1 {
		fs.Usage()
		return 2
	}
	if f, ok := stdin.(*os.File); ok {
		if info, err := f.Stat(); err == nil && info.Mode()&os.ModeCharDevice != 0 {
			fmt.Fprintln(stderr, "Enter the value, then press Ctrl-D:")
		}
	}
	value, err := io.ReadAll(io.LimitReader(stdin, secrets.MaxValueSize+1))
	if err != nil {
		fmt.Fprintf(stderr, "controller: reading value: %v\n", err)
		return 1
	}
	// "echo value | ..." adds a newline nobody means to store.
	value = bytes.TrimSuffix(bytes.TrimSuffix(value, []byte("\n")), []byte("\r"))

	return withSecrets(c, stderr, func(ctx context.Context, m *secrets.Manager) error {
		if err := m.Set(ctx, pos[0], value, secrets.Policy{
			Repos: repos, Pools: pools, Refs: refs, AllowPullRequests: *allowPR, Description: *desc,
		}); err != nil {
			return err
		}
		fmt.Fprintf(stdout, "Secret %s saved (%d bytes).\n", pos[0], len(value))
		return nil
	})
}

func secretList(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("secret list", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	if err := fs.Parse(args); err != nil {
		return 2
	}
	return withSecrets(c, stderr, func(ctx context.Context, m *secrets.Manager) error {
		list, err := m.Store.ListSecrets(ctx)
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tREPOS\tPOOLS\tREFS\tPULL REQUESTS\tUPDATED\tDESCRIPTION")
		for _, s := range list {
			refs, pr := "any", "no"
			if len(s.Refs) > 0 {
				refs = strings.Join(s.Refs, ",")
			}
			if s.AllowPullRequests {
				pr = "yes"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s.Name, strings.Join(s.Repos, ","), strings.Join(s.Pools, ","),
				refs, pr, s.UpdatedAt.Local().Format(time.DateTime), s.Description)
		}
		return tw.Flush()
	})
}

func secretDelete(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("secret delete", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	pos, err := parseWithArgs(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(stderr, "Usage: controller secret delete [options] NAME")
		return 2
	}
	return withSecrets(c, stderr, func(ctx context.Context, m *secrets.Manager) error {
		n, err := m.Store.DeleteSecret(ctx, pos[0])
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("no secret named %s", pos[0])
		}
		fmt.Fprintf(stdout, "Secret %s deleted.\n", pos[0])
		return nil
	})
}

func secretAccess(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("secret access", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var c common
	c.register(fs)
	limit := fs.Int("n", 50, "number of entries")
	pos, err := parseWithArgs(fs, args)
	if err != nil || len(pos) != 1 {
		fmt.Fprintln(stderr, "Usage: controller secret access [options] NAME")
		return 2
	}
	return withSecrets(c, stderr, func(ctx context.Context, m *secrets.Manager) error {
		list, err := m.Store.ListSecretAccesses(ctx, db.ListSecretAccessesParams{SecretName: pos[0], Limit: int32(*limit)})
		if err != nil {
			return err
		}
		tw := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TIME\tRESULT\tRUN\tJOB\tWORKER\tREASON")
		for _, a := range list {
			result := "denied"
			if a.Granted {
				result = "granted"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", a.At.Local().Format(time.DateTime), result,
				shortID(a.RunID), shortID(a.JobID), shortID(a.WorkerID), a.Reason)
		}
		return tw.Flush()
	})
}

func shortID[T fmt.Stringer](id *T) string {
	if id == nil {
		return "-"
	}
	return (*id).String()[:8]
}
