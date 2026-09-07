// Command absort sorts an abtab record stream by one or more key fields,
// shelling out to the system "sort" for the actual ordering.
package main

import (
	"github.com/alecthomas/kong"
	"github.com/kyleburton/go-abtab/internal/cli"
	"github.com/kyleburton/go-abtab/pkg/abtab"
)

var CLI struct {
	cli.IOFlags
	Key         string `short:"k" required:"" help:"Comma separated sort key field names."`
	NumericSort bool   `short:"n" name:"numeric-sort" help:"Sort according to numeric value."`
	Reverse     bool   `short:"r" help:"Reverse sort order."`
	Tmpdir      string `default:"/tmp" help:"Directory to use for temporary files."`
	Verbose     bool   `short:"v" help:"Be verbose (to stderr)."`
}

func main() {
	kong.Parse(&CLI, kong.Description("Sort an abtab record stream by one or more key fields."))
	abtab.Verbose = CLI.Verbose

	inputURI, outputURI := CLI.IOFlags.Resolve()

	cleanup, err := cli.UsePagerIfStdoutIsTerminal(outputURI)
	cli.Fatal(err)
	defer cleanup()

	inp, err := abtab.ParseURL(inputURI)
	cli.Fatal(err)

	outp, err := abtab.ParseURL(outputURI)
	cli.Fatal(err)

	cli.Fatal(abtab.AbtabSort(inp, outp, CLI.Key, CLI.Tmpdir, CLI.NumericSort, CLI.Reverse))
}
