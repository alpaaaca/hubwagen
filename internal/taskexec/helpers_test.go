package taskexec

import "os/exec"

func runQuiet(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	return string(out), err
}
