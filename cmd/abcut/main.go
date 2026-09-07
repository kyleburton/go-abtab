// Command abcut extracts and reorders fields from an abtab record stream by
// name or by 1-based position.
package main

import (
	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.IOFlags
	Fields  string `short:"f" default:"1" help:"Comma separated field names or 1-based positions to keep, in order."`
	Verbose bool   `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description(cli.WithEncodingsHelp("Cut fields out of an abtab record stream, by name or position.")))
	abtab.Verbose = CLI.Verbose

	inputURI, outputURI := CLI.IOFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(outputURI)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	outp, err := abtab.ParseURL(outputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabCut(inp, outp, CLI.Fields))
}
