package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"

	opificav1 "github.com/opifica/opifica/gen/opifica/v1"
)

const zeroSHA = "0000000000000000000000000000000000000000"

// hook implements `opifica hook post-receive`: git runs it after a push with
// one "old new ref" line per updated ref on stdin. Configuration comes from
// the environment (OPIFICA_CONTROLLER, OPIFICA_CA_FINGERPRINT, OPIFICA_REPO,
// OPIFICA_TRIGGER_TOKEN), set by the hook script.
//
// It never fails: the push has already been accepted, and a broken CI must
// not look like a broken push. Problems are printed for the pusher.
func hook(args []string, stdin io.Reader, stdout io.Writer) int {
	if len(args) != 1 || args[0] != "post-receive" {
		fmt.Fprintln(stdout, "opifica: usage: opifica hook post-receive")
		return 0
	}
	var c controllerConfig
	c.fromEnv()
	client, err := c.client()
	if err != nil {
		fmt.Fprintf(stdout, "opifica: not triggering a run: %v\n", err)
		return 0
	}

	sc := bufio.NewScanner(stdin)
	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 3 {
			continue
		}
		newSHA, ref := fields[1], fields[2]
		var event string
		switch {
		case strings.Trim(newSHA, "0") == "":
			continue // the ref was deleted
		case strings.HasPrefix(ref, "refs/heads/"):
			event = "push"
		case strings.HasPrefix(ref, "refs/tags/"):
			event = "tag"
		default:
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		resp, err := client.Trigger(ctx, connect.NewRequest(&opificav1.TriggerRequest{
			Repository: c.repo, Event: event, Ref: ref, CommitSha: newSHA,
		}))
		cancel()
		name := strings.TrimPrefix(strings.TrimPrefix(ref, "refs/heads/"), "refs/tags/")
		switch {
		case err != nil:
			fmt.Fprintf(stdout, "opifica: %s: could not trigger a run: %v\n", name, err)
		case resp.Msg.GetRunId() == "":
			fmt.Fprintf(stdout, "opifica: %s: %s\n", name, resp.Msg.GetMessage())
		default:
			fmt.Fprintf(stdout, "opifica: %s: run %s started in pool %s (%d jobs)\n",
				name, resp.Msg.GetRunId(), resp.Msg.GetPool(), resp.Msg.GetJobs())
		}
	}
	return 0
}
