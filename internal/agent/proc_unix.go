//go:build unix

package agent

import (
	"os/exec"
	"syscall"
)

// killTree runs the command in its own process group and makes cancellation
// kill the whole group, so background processes a step started die with it.
func killTree(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
