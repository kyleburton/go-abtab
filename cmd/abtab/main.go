package main

import (
	"flag"
	"fmt"
	"github.com/kyleburton/go-abtab/pkg/abtab"
	"os"
)

func main() {
	var task, input, output, expression, sortKey, tmpdir, sortNumeric, sortReverse, fields string
	flag.StringVar(&task, "task", "cat", "Task (App) to run.")
	flag.StringVar(&input, "input", "tab:///dev/stdin", "Input Source")
	flag.StringVar(&input, "i", "tab:///dev/stdin", "Input Source")
	flag.StringVar(&output, "output", "tab:///dev/stdout", "Output Destination")
	flag.StringVar(&output, "o", "tab:///dev/stdout", "Output Destination")
	flag.StringVar(&expression, "expression", "true", "Specify Expression")
	flag.StringVar(&expression, "e", "true", "Specify Expression")

	flag.StringVar(&sortKey, "key", "", "Sort Key Fields, comma separated")
	flag.StringVar(&sortKey, "k", "", "Sort Key Fields, comma separated")
	flag.StringVar(&sortNumeric, "numeric-sort", "false", "Sort according to numerical value.")
	flag.StringVar(&sortNumeric, "n", "false", "head,tail: number of lines; sort: Sort according to numerical value.")
	flag.StringVar(&sortReverse, "reverse", "false", "Reverse sort order")
	flag.StringVar(&sortReverse, "r", "false", "Reverse sort order")

	// re-use of '-n'
	flag.StringVar(&sortNumeric, "numLines", "10", "Number of lines (head, tail).")

	flag.StringVar(&tmpdir, "tmpdir", "/tmp", "Directory to use for temporary files.")
	flag.StringVar(&fields, "fields", "1", "cut: fields")
	flag.StringVar(&fields, "f", "1", "cut: fields")

	flag.BoolVar(&abtab.Verbose, "v", false, "Be verbose (to stderr)")

	flag.Parse()

	var err error
	abtab.CmdlineOpts["input"], err = abtab.ParseURL(input)
	if err != nil {
		panic(err)
	}

	abtab.CmdlineOpts["output"], err = abtab.ParseURL(output)
	if err != nil {
		panic(err)
	}

	abtab.CmdlineOpts["expression"] = expression
	abtab.CmdlineOpts["sortKey"] = sortKey
	abtab.CmdlineOpts["sortNumeric"] = sortNumeric
	abtab.CmdlineOpts["numLines"] = sortNumeric // re-use of -n
	abtab.CmdlineOpts["sortReverse"] = sortReverse
	abtab.CmdlineOpts["tmpdir"] = tmpdir
	abtab.CmdlineOpts["fields"] = fields

	if abtab.Verbose {
		fmt.Fprintf(os.Stderr, "CmdlineOpts: %s\n", abtab.CmdlineOpts)
	}

	switch {
	case "cat" == task:
		abtab.AbtabCat(flag.Args())
		break
	case "view" == task:
		abtab.AbtabView(flag.Args())
		break
	case "fill-rates" == task:
		abtab.AbtabFillRates(flag.Args())
		break
	case "grep" == task:
		abtab.AbtabGrep(flag.Args())
		break
	case "sort" == task:
		abtab.AbtabSort(flag.Args())
		break
	case "head" == task:
		abtab.AbtabHead(flag.Args())
		break
	case "tail" == task:
		abtab.AbtabTail(flag.Args())
		break
	case "cut" == task:
		abtab.AbtabCut(flag.Args())
		break
	default:
		fmt.Fprintf(os.Stderr, "Error: unrecognized task: %s\n", task)
		os.Exit(1)
	}

	/**
	  runtime.GOMAXPROCS(4);
	*/
}
