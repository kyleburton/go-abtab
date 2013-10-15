/*
Package abtab implements a library for working with tabular data: streams of records.


*/
package abtab

import (
  "fmt"
  "os"
  "net/url"
  "io/ioutil"
)

/*
  Map for holding parsed command line parameters.
*/
type CmdlineOptsType map[string] interface{}

/*
  Map for holding parsed command line parameters.
*/
var CmdlineOpts CmdlineOptsType = CmdlineOptsType {
  "input":  "/dev/stdin",
  "output": "/dev/stdout",
}

var Verbose bool = false

// Customized error type.
type AbtabError struct {
  Message string
  CausedBy error
}

// Implement the Error interface for AbtabError
func (e AbtabError) Error () string {
  return e.Message
}

// Function type for driver's write capability.
type WriteRecordFn func (*Rec) error
// Function type driver's Close function.
type CloseFn       func () error

// Function type for stream filter predicates.
type AbtabFilterFn func (*Rec) (bool, error)

// Parsed URL struct.
type AbtabURL struct {
  OriginalUrl      string
  Url              *url.URL
  Stream           *PushBackRecStream
  WriteRecord      WriteRecordFn
  Close            CloseFn
  RecordSeparator  string
  FieldSeparator   string
  Header           []string
  HeaderMap        map[string]int
  SkipLines        int64
}

// Set the header on a source.
func (self *AbtabURL) SetHeader (header []string) {
  self.Header = header
  self.HeaderMap = make(map[string]int)
  for idx, fname := range header {
    //fmt.Printf("SetHeader: %s=%d\n", fname, idx)
    self.HeaderMap[fname] = idx
  }
}

// To string helper for debugging.
func (self *AbtabURL) String () string {
  return fmt.Sprintf("AbtabUrl{OriginalUrl=%s; Scheme=%s; Host=%s; User=%s; Path=%s; Query=%s}",
    self.OriginalUrl,
    self.Url.Scheme,
    self.Url.Host,
    self.Url.User,
    self.Url.Path,
    self.Url.RawQuery,
  )
}

// Parse an abtab URL into an AbtabURL struct.
func ParseURL (u string) (*AbtabURL, error) {
  url, err := url.Parse(u)
  if err != nil {
    return nil, AbtabError {Message: fmt.Sprintf("Error: invalid url: '%s' :: %s", u, err), CausedBy: err }
  }

  return &AbtabURL {
    OriginalUrl: u,
    Url:         url,
  }, nil
}

// Open a source for reading.
func (self *AbtabURL) OpenRead () error {
  switch {
  case "tab" == self.Url.Scheme:
    self.TabOpenRead()
    return nil
    break
  case "" == self.Url.Scheme:
    self.TabOpenRead()
    return nil
    break
  default:
    return AbtabError{Message: fmt.Sprintf("Error: unrecognized scheme: '%s'", self.Url.Scheme)}
    break
  case "csv" == self.Url.Scheme:
    self.CsvOpenRead()
    break;
//  case "fixed" == self.Url.Scheme:
//    self.FixedOpenRead()
//    break;
  case "pg" == self.Url.Scheme:
    self.PgOpenRead()
    break;
//  case "mysql" == self.Url.Scheme:
//    self.MysqlOpenRead()
//    break;
//    return nil
  }
  return nil
}

// Open a source for writing.
func (self *AbtabURL) OpenWrite () error {
  switch {
  case "tab" == self.Url.Scheme:
    self.TabOpenWrite()
    return nil
    break
  case "" == self.Url.Scheme:
    self.TabOpenWrite()
    return nil
    break
  default:
    return AbtabError{Message: fmt.Sprintf("Error: unrecognized scheme: '%s'", self.Url.Scheme)}
    break
  case "csv" == self.Url.Scheme:
    self.CsvOpenWrite()
    break;
//  case "fixed" == self.Url.Scheme:
//    self.FixedOpenWrite()
//    break;
//  case "pg" == self.Url.Scheme:
//    self.PgOpenWrite()
//    break;
//  case "mysql" == self.Url.Scheme:
//    self.MysqlOpenWrite()
//    break;
//    return nil
  }
  return nil
}

// Generate a temporary file name.
func TempFileName () (string, error) {
  tmpdir := CmdlineOpts["tmpdir"].(string)
  file, err := ioutil.TempFile(tmpdir, "abtab.tmp.")
  if err != nil {
    return "", err
  }

  name := file.Name()
  file.Close()
  os.Remove(name)

  return name, nil
}
