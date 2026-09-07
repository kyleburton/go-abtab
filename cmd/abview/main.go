// Command abview renders an abtab record stream in a "portrait" layout, one
// field per line, for easy visual inspection of wide records.
package main

import (
	"os"

	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.InputFlags
	Verbose bool `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description("View an abtab record stream in portrait layout."))
	abtab.Verbose = CLI.Verbose

	inputURI := CLI.InputFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(cli.DefaultOutput)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabView(inp, os.Stdout))
}
