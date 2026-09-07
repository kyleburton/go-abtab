// Command abtail emits the header plus the last N records of an abtab
// record stream.
package main

import (
	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.IOFlags
	Lines   int64 `short:"n" default:"10" help:"Number of records to emit."`
	Verbose bool  `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description("Emit the header plus the last N records of an abtab record stream."))
	abtab.Verbose = CLI.Verbose

	inputURI, outputURI := CLI.IOFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(outputURI)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	outp, err := abtab.ParseURL(outputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabTail(inp, outp, CLI.Lines))
}
