package cli

import (
	"os"
	"os/exec"
	"strings"
)

func isTerminal(f *os.File) bool {
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}

func targetsStdout(outputURI string) bool {
	return strings.HasSuffix(outputURI, "/dev/stdout")
}

// UsePagerIfStdoutIsTerminal pipes the process's stdout through a pager ($PAGER, falling back
// to "less") when stdout is attached to a terminal and outputURI resolves to
// stdout. It returns a cleanup func that must run (typically via defer)
// after all output has been written, to flush the pipe and wait for the
// pager to exit. When paging doesn't apply, the returned cleanup is a no-op.
func UsePagerIfStdoutIsTerminal(outputURI string) (cleanup func(), err error) {
	noop := func() {}

	if !targetsStdout(outputURI) || !isTerminal(os.Stdout) {
		return noop, nil
	}

	pagerPath := os.Getenv("PAGER")
	if pagerPath == "" {
		pagerPath = "less"
	}

	r, w, err := os.Pipe()
	if err != nil {
		return noop, err
	}

	cmd := exec.Command(pagerPath)
	cmd.Stdin = r
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		r.Close()
		w.Close()
		return noop, err
	}
	r.Close()

	realStdout := os.Stdout
	os.Stdout = w

	return func() {
		os.Stdout = realStdout
		w.Close()
		cmd.Wait()
	}, nil
}
