// Command abgrep filters an abtab record stream using a boolean expression
// evaluated per-record, with each field bound as a variable.
package main

import (
	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.IOFlags
	Expression string `short:"e" default:"true" help:"Boolean expression evaluated per record; fields are bound as variables."`
	Verbose    bool   `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description("Filter an abtab record stream by an expression."))
	abtab.Verbose = CLI.Verbose

	inputURI, outputURI := CLI.IOFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(outputURI)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	outp, err := abtab.ParseURL(outputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabGrep(inp, outp, CLI.Expression))
}
