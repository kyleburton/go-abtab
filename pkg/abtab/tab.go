package abtab

import (
  "os"
  "fmt"
  "bufio"
  "strings"
  "strconv"
)

func TabRecStream(source *AbtabURL, fname string, out chan *Rec, headerProvided bool) {
  var file *os.File
  var err error


  if fname == "/dev/stdin" || fname == "//dev/stdin" {
    file = os.Stdin
  } else {
    //fmt.Printf("Opening: %s\n", file)
    file, err = os.Open(fname)
    if err != nil {
      panic(fmt.Sprintf("Error opening file: %s : %s", fname, err))
    }
    defer file.Close()
  }

  var numLines int64 = 0
  if !headerProvided {
    numLines = -1
  }
  scanner := bufio.NewScanner(file)
  for scanner.Scan() {
    numLines = numLines + 1
    // turn \N into an empty string for any field where it appears
    fields := strings.Split(scanner.Text(), "\t")
    for ii, _ := range fields {
      if fields[ii] == "\\N" {
        fields[ii] = ""
      }
    }
    //fmt.Fprintf(os.Stderr, "TabRecStream: Rec.LineNum=%s\n", numLines)
    out <- &Rec{
      Source:  source,
      LineNum: numLines,
      Fields:  fields,
    }
  }

  if err := scanner.Err(); err != nil {
    panic(fmt.Sprintf("Error reading from file %s : %s", fname, err))
  }

  close(out)
}

func TabFilePath (u *AbtabURL) string {
  var fileName = ""
  if len(u.Url.Host) > 0 {
    fileName = u.Url.Host + "/"
  }

  fileName += u.Url.Path
  fileName = strings.Replace(fileName, "//", "/", -1)

  if strings.HasSuffix(fileName, "/") {
    fileName = fileName[0:len(fileName)-1]
  }

  return fileName
}

func (self *AbtabURL) TabOpenRead () error {
  var fileName = TabFilePath(self)
  var err error
  self.Stream = &PushBackRecStream {
    Name:      fileName,
    Recs:      make(chan *Rec),
    LastRecs:  make([]*Rec, 0),
  }

  qs := self.Url.Query()
  header, headerProvided := qs["header"]

  go TabRecStream(self, fileName, self.Stream.Recs, headerProvided)


  if headerProvided {
    self.SetHeader(strings.Split(header[0], ","))
  } else {
    r, ok := <-self.Stream.Recs
    if !ok {
      // empty stream
      return nil
    }
    self.Header = r.Fields
  }

  skipLines, ok := qs["skipLines"]
  if ok {
    self.SkipLines, err = strconv.ParseInt(skipLines[0], 10, 64)
    if err != nil {
      return AbtabError{Message: fmt.Sprintf("Error parsing skipLines: %s", skipLines[0]), CausedBy: err}
    }
  }

  self.WriteRecord = func (r *Rec) error {
    return AbtabError{Message: "Error: Tab: not open for writing!"}
  }
  self.Close = func () error {
    return nil
  }
  // NB: pull these optionally from the QueryString
  self.FieldSeparator  = "\t"
  self.RecordSeparator = "\n"
  return nil
}

func (self *AbtabURL) TabOpenWrite () error {
  var fileName = TabFilePath(self)
  var file *os.File
  var err error
  self.Stream = &PushBackRecStream {
    Name:            fileName,
    Recs:            make(chan *Rec),
    LastRecs:        make([]*Rec, 0),
  }

  if (fileName == "/dev/stdout" || fileName == "//dev/stdout") {
    file = os.Stdout
  } else {
    file, err = os.Create(fileName)
    if err != nil {
      panic(fmt.Sprintf("Error opening file: %s : %s", fileName, err))
    }
  }

  qs := self.Url.Query()
  _, noHeader := qs["-header"]

  // NB: pull these optionally from the QueryString
  self.FieldSeparator  = "\t"
  self.RecordSeparator = "\n"

  if !noHeader {
    _, err = file.Write([]byte(strings.Join(self.Header, self.FieldSeparator)))
    if err != nil {
      return err
    }
    _, err = file.Write([]byte(self.RecordSeparator))
    if err != nil {
      return err
    }
  }

  self.WriteRecord = func (r *Rec) error {
    _, err := file.Write([]byte(strings.Join(r.Fields, self.FieldSeparator)))
    if err != nil {
      return err
    }
    _, err = file.Write([]byte(self.RecordSeparator))
    if err != nil {
      return err
    }
    return nil
  }

  self.Close = func () error {
    file.Close()
    return nil
  }

  return nil
}
