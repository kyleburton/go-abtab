// Command abfillrates reports, for each field in an abtab record stream, how
// many records have a non-empty value for that field.
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
	kong.Parse(&CLI, kong.Description("Report field fill rates for an abtab record stream."))
	abtab.Verbose = CLI.Verbose

	inputURI := CLI.InputFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(cli.DefaultOutput)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabFillRates(inp, os.Stdout))
}
