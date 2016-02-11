package main

import (
	"flag"
	"fmt"
	"github.com/kyleburton/go-abtab/pkg/abtab"
	"os"
	"path"
	"strings"
)

type CommandLineOptionsStruct struct {
	Task           string
	Input          string
	Output         string
	Expression     string
	SortKey        string
	Tmpdir         string
	SortViaNumeric string
	SortReverse    string
	Fields         string
	Args           []string
}

var DefaultInput = "tab:///dev/stdin"
var DefaultOutput = "tab:///dev/stdout"

var CommandLineOptions CommandLineOptionsStruct = CommandLineOptionsStruct{}

func LooksLikeUri(s string) bool {
	return strings.Contains(CommandLineOptions.Input, "://")
}

func FileNameToUri(fname string) string {
	if LooksLikeUri(fname) {
		return fname
	}

	ext := strings.ToLower(path.Ext(fname))
	if len(ext) > 0 {
		ext = ext[1:]
	}
	// fmt.Fprintf(os.Stderr, "FileNameToUri[%s] ext=%s\n", fname, ext)
	switch {
	case "tab" == ext:
		return strings.Join([]string{"tab://", fname}, "")
		break
	case "csv" == ext:
		return strings.Join([]string{"csv://", fname}, "")
		break
	default:
		return fname
	}

	return fname
}

func FindInputUri() {
	if CommandLineOptions.Input != DefaultInput && LooksLikeUri(CommandLineOptions.Input) {
		return
	}

	if CommandLineOptions.Input != DefaultInput {
		CommandLineOptions.Input = FileNameToUri(CommandLineOptions.Input)
		return
	}

	if CommandLineOptions.Input == DefaultInput && len(CommandLineOptions.Args) > 0 {
		CommandLineOptions.Input = FileNameToUri(CommandLineOptions.Args[0])
		CommandLineOptions.Args = CommandLineOptions.Args[1:]
	}

}

func FindOutputUri() {
	if CommandLineOptions.Output != DefaultOutput && LooksLikeUri(CommandLineOptions.Output) {
		return
	}

	if CommandLineOptions.Output != DefaultOutput {
		CommandLineOptions.Output = FileNameToUri(CommandLineOptions.Output)
		return
	}

	if CommandLineOptions.Output == DefaultOutput && len(CommandLineOptions.Args) > 0 {
		CommandLineOptions.Output = FileNameToUri(CommandLineOptions.Args[0])
		CommandLineOptions.Args = CommandLineOptions.Args[1:]
	}

}

func main() {
	flag.StringVar(&CommandLineOptions.Task, "task", "cat", "Task (App) to run.")
	flag.StringVar(&CommandLineOptions.Input, "input", DefaultInput, "Input Source")
	flag.StringVar(&CommandLineOptions.Input, "i", DefaultInput, "Input Source")
	flag.StringVar(&CommandLineOptions.Output, "output", DefaultOutput, "Output Destination")
	flag.StringVar(&CommandLineOptions.Output, "o", DefaultOutput, "Output Destination")
	flag.StringVar(&CommandLineOptions.Expression, "expression", "true", "Specify Expression")
	flag.StringVar(&CommandLineOptions.Expression, "e", "true", "Specify Expression")

	flag.StringVar(&CommandLineOptions.SortKey, "key", "", "Sort Key Fields, comma separated")
	flag.StringVar(&CommandLineOptions.SortKey, "k", "", "Sort Key Fields, comma separated")
	flag.StringVar(&CommandLineOptions.SortViaNumeric, "numeric-sort", "false", "Sort according to numerical value.")
	flag.StringVar(&CommandLineOptions.SortViaNumeric, "n", "false", "head,tail: number of lines; sort: Sort according to numerical value.")
	flag.StringVar(&CommandLineOptions.SortReverse, "reverse", "false", "Reverse sort order")
	flag.StringVar(&CommandLineOptions.SortReverse, "r", "false", "Reverse sort order")

	// re-use of '-&n'
	flag.StringVar(&CommandLineOptions.SortViaNumeric, "numLines", "10", "Number of lines (head, tail).")

	flag.StringVar(&CommandLineOptions.Tmpdir, "tmpdir", "/tmp", "Directory to use for temporary files.")
	flag.StringVar(&CommandLineOptions.Fields, "fields", "1", "cut: fields")
	flag.StringVar(&CommandLineOptions.Fields, "f", "1", "cut: fields")

	flag.BoolVar(&abtab.Verbose, "v", false, "Be verbose (to stderr)")

	flag.Parse()
	CommandLineOptions.Args = flag.Args()

	FindInputUri()
	FindOutputUri()

	var err error
	abtab.CmdlineOpts["input"], err = abtab.ParseURL(CommandLineOptions.Input)
	if err != nil {
		panic(err)
	}

	abtab.CmdlineOpts["output"], err = abtab.ParseURL(CommandLineOptions.Output)
	if err != nil {
		panic(err)
	}

	abtab.CmdlineOpts["expression"] = CommandLineOptions.Expression
	abtab.CmdlineOpts["sortKey"] = CommandLineOptions.SortKey
	abtab.CmdlineOpts["sortNumeric"] = CommandLineOptions.SortViaNumeric
	abtab.CmdlineOpts["numLines"] = CommandLineOptions.SortViaNumeric // re-use of -n
	abtab.CmdlineOpts["sortReverse"] = CommandLineOptions.SortReverse
	abtab.CmdlineOpts["tmpdir"] = CommandLineOptions.Tmpdir
	abtab.CmdlineOpts["fields"] = CommandLineOptions.Fields

	if abtab.Verbose {
		fmt.Fprintf(os.Stderr, "CmdlineOpts: %s\n", abtab.CmdlineOpts)
	}

	switch {
	case "cat" == CommandLineOptions.Task:
		abtab.AbtabCat(CommandLineOptions.Args)
		break
	case "view" == CommandLineOptions.Task:
		abtab.AbtabView(CommandLineOptions.Args)
		break
	case "fillrates" == CommandLineOptions.Task || "fill-rates" == CommandLineOptions.Task:
		abtab.AbtabFillRates(CommandLineOptions.Args)
		break
	case "grep" == CommandLineOptions.Task:
		abtab.AbtabGrep(CommandLineOptions.Args)
		break
	case "sort" == CommandLineOptions.Task:
		abtab.AbtabSort(CommandLineOptions.Args)
		break
	case "head" == CommandLineOptions.Task:
		abtab.AbtabHead(CommandLineOptions.Args)
		break
	case "tail" == CommandLineOptions.Task:
		abtab.AbtabTail(CommandLineOptions.Args)
		break
	case "cut" == CommandLineOptions.Task:
		abtab.AbtabCut(CommandLineOptions.Args)
		break
	default:
		fmt.Fprintf(os.Stderr, "Error: unrecognized task: %s\n", CommandLineOptions.Task)
		os.Exit(1)
	}

	/**
	  runtime.GOMAXPROCS(4);
	*/
}
