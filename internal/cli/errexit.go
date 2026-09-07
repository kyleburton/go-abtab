package cli

import (
	"fmt"
	"os"
)

// Fatal prints err to stderr and exits the process with status 1. It's a
// no-op when err is nil.
func Fatal(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	os.Exit(1)
}
