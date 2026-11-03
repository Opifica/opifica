//go:build windows

package agent

import "os/exec"

// killTree kills the command on cancellation.
//
// TODO: put the process in a job object so its children are killed too. The
// VM is destroyed after every job, so leftovers can't outlive the job, but
// a later step in the same job could still see them.
func killTree(cmd *exec.Cmd) {
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return cmd.Process.Kill()
	}
}
