// Package cli holds flag structures and helpers shared by the abtab command
// binaries under cmd/.
package cli

import (
	"path"
	"strings"
)

const (
	DefaultInput  = "tab:///dev/stdin"
	DefaultOutput = "tab:///dev/stdout"
)

// IOFlags is embedded by commands that read a stream and write a stream
// (abcat, abcut, abgrep, abhead, abtail, absort).
type IOFlags struct {
	Input  string   `short:"i" default:"tab:///dev/stdin" help:"Input source URI or file path."`
	Output string   `short:"o" default:"tab:///dev/stdout" help:"Output destination URI or file path."`
	Args   []string `arg:"" optional:"" help:"Positional [input [output]] paths, alternative to -i/-o."`
}

// Resolve applies the same fallback rules as the original CLI: an explicit
// -i/-o wins; otherwise positional arguments are consumed in order (input,
// then output) and a file's extension (.tab/.csv) is used to guess a scheme.
func (f *IOFlags) Resolve() (inputURI, outputURI string) {
	args := f.Args
	inputURI = resolveOne(f.Input, DefaultInput, &args)
	outputURI = resolveOne(f.Output, DefaultOutput, &args)
	return inputURI, outputURI
}

// InputFlags is embedded by commands that only read a stream (abview,
// abfillrates).
type InputFlags struct {
	Input string   `short:"i" default:"tab:///dev/stdin" help:"Input source URI or file path."`
	Args  []string `arg:"" optional:"" help:"Positional input path, alternative to -i."`
}

// Resolve applies the same fallback rules as IOFlags.Resolve, for the
// input-only case.
func (f *InputFlags) Resolve() (inputURI string) {
	args := f.Args
	return resolveOne(f.Input, DefaultInput, &args)
}

func resolveOne(explicit, def string, args *[]string) string {
	if explicit != def {
		return fileNameToUri(explicit)
	}
	if len(*args) > 0 {
		uri := fileNameToUri((*args)[0])
		*args = (*args)[1:]
		return uri
	}
	return explicit
}

func looksLikeUri(s string) bool {
	return strings.Contains(s, "://")
}

func fileNameToUri(fname string) string {
	if looksLikeUri(fname) {
		return fname
	}

	ext := strings.ToLower(path.Ext(fname))
	if len(ext) > 0 {
		ext = ext[1:]
	}

	switch ext {
	case "tab":
		return "tab://" + fname
	case "csv":
		return "csv://" + fname
	default:
		return fname
	}
}
