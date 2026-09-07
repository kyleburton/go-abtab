/*
Package abtab implements a library for working with tabular data: streams of records.
*/
package abtab

import (
	"fmt"
	"net/url"
	"os"
)

// Verbose enables diagnostic logging to stderr across the package.
var Verbose bool = false

// Customized error type.
type AbtabError struct {
	Message  string
	CausedBy error
}

// Implement the Error interface for AbtabError
func (e AbtabError) Error() string {
	return e.Message
}

// Unwrap exposes CausedBy so errors.Is/errors.As work against AbtabError.
func (e AbtabError) Unwrap() error {
	return e.CausedBy
}

// Function type for driver's write capability.
type WriteRecordFn func(*Rec) error

// Function type driver's Close function.
type CloseFn func() error

// Function type for stream filter predicates.
type AbtabFilterFn func(*Rec) (bool, error)

// Parsed URL struct.
type AbtabURL struct {
	OriginalUrl     string
	Url             *url.URL
	Stream          *PushBackRecStream
	WriteRecord     WriteRecordFn
	Close           CloseFn
	RecordSeparator string
	FieldSeparator  string
	Header          []string
	HeaderMap       map[string]int
	SkipLines       int64
}

// Set the header on a source.
func (self *AbtabURL) SetHeader(header []string) {
	self.Header = header
	self.HeaderMap = make(map[string]int)
	for idx, fname := range header {
		//fmt.Printf("SetHeader: %s=%d\n", fname, idx)
		self.HeaderMap[fname] = idx
	}
}

// To string helper for debugging.
func (self *AbtabURL) String() string {
	return fmt.Sprintf("AbtabURL{OriginalUrl=%s; Scheme=%s; Host=%s; User=%s; Path=%s; Query=%s}",
		self.OriginalUrl,
		self.Url.Scheme,
		self.Url.Host,
		self.Url.User,
		self.Url.Path,
		self.Url.RawQuery,
	)
}

// Parse an abtab URL into an AbtabURL struct.
func ParseURL(u string) (*AbtabURL, error) {
	url, err := url.Parse(u)
	if err != nil {
		return nil, AbtabError{Message: fmt.Sprintf("Error: invalid url: '%s' :: %s", u, err), CausedBy: err}
	}

	return &AbtabURL{
		OriginalUrl: u,
		Url:         url,
	}, nil
}

// Open a source for reading.
func (self *AbtabURL) OpenRead() error {
	switch self.Url.Scheme {
	case "", "tab":
		return self.TabOpenRead()
	case "csv":
		return self.CsvOpenRead()
	case "fixed":
		return self.FixedWidthOpenRead()
	case "pg":
		return self.PgOpenRead()
	default:
		return AbtabError{Message: fmt.Sprintf("Error: unrecognized scheme: '%s'", self.Url.Scheme)}
	}
}

// Open a source for writing.
func (self *AbtabURL) OpenWrite() error {
	switch self.Url.Scheme {
	case "", "tab":
		return self.TabOpenWrite()
	case "csv":
		return self.CsvOpenWrite()
	case "fixed":
		return self.FixedWidthOpenWrite()
	default:
		return AbtabError{Message: fmt.Sprintf("Error: unrecognized scheme: '%s'", self.Url.Scheme)}
	}
}

// Generate a temporary file name.
func TempFileName(tmpdir string) (string, error) {
	file, err := os.CreateTemp(tmpdir, "abtab.tmp.")
	if err != nil {
		return "", err
	}

	name := file.Name()
	file.Close()
	os.Remove(name)

	return name, nil
}
