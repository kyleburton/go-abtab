// Command abcat concatenates an abtab record stream to an output stream,
// converting between encodings (tab, csv, ...) along the way.
package main

import (
	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.IOFlags
	Verbose bool `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description(cli.WithEncodingsHelp("Concatenate an abtab record stream, like cat.")))
	abtab.Verbose = CLI.Verbose

	inputURI, outputURI := CLI.IOFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(outputURI)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	outp, err := abtab.ParseURL(outputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabCat(inp, outp))
}
