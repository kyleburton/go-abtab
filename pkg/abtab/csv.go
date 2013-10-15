package abtab

import (
  "os"
  "fmt"
  "bufio"
  "strings"
  "strconv"
  "encoding/csv"
)

func CsvRecStream(source *AbtabURL, fname string, out chan *Rec) error {
  var file *os.File
  var err error


  if fname == "/dev/stdin" || fname == "//dev/stdin" {
    file = os.Stdin
  } else {
    if file, err = os.Open(fname); err != nil {
      panic(fmt.Sprintf("Error opening file: %s : %s", fname, err))
    }
    defer file.Close()
  }

  scanner := bufio.NewScanner(file)

  var idx int64
  //fmt.Printf("CsvRecStream: skipping: %d lines\n", source.SkipLines)
  for idx = 0; idx < source.SkipLines; idx += 1 {
    scanner.Scan()
  }

  var numLines int64 = 0
  for scanner.Scan() {
    numLines = numLines + 1
    // turn \N into an empty string for any field where it appears
    fields, err := csv.NewReader(strings.NewReader(scanner.Text())).Read()
    if err != nil {
      panic(err)
    }

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
  return nil
}

func (self *AbtabURL) CsvOpenRead () error {
  var fileName = TabFilePath(self)
  var err error
  self.Stream = &PushBackRecStream {
    Name:      fileName,
    Recs:      make(chan *Rec),
    LastRecs:  make([]*Rec, 0),
  }
  qs := self.Url.Query()

  skipLines, ok := qs["skipLines"]
  if ok {
    self.SkipLines, err = strconv.ParseInt(skipLines[0], 10, 64)
    if err != nil {
      return AbtabError{Message: fmt.Sprintf("Error parsing skipLines: %s", skipLines[0]), CausedBy: err}
    }
  }

  go CsvRecStream(self, fileName, self.Stream.Recs)

  header, ok := qs["header"]
  if ok {
    self.SetHeader(strings.Split(header[0], ","))
  } else {
    r := <-self.Stream.Recs
    self.SetHeader(r.Fields)
  }

  self.WriteRecord = func (r *Rec) error {
    return AbtabError{Message: "Error: Csv: not open for writing!"}
  }

  self.Close = func () error {
    return nil
  }
  // NB: pull these optionally from the QueryString
  self.FieldSeparator  = "\t"
  self.RecordSeparator = "\n"
  return nil
}

func (self *AbtabURL) CsvOpenWrite () error {
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

  csvWriter := csv.NewWriter(file)

  qs := self.Url.Query()
  _, noHeader := qs["-header"]

  // NB: pull these optionally from the QueryString
  self.FieldSeparator  = "\t"
  self.RecordSeparator = "\n"

  if !noHeader {
    err = csvWriter.Write(self.Header)
    if err != nil {
      return err
    }
  }

  self.WriteRecord = func (r *Rec) error {
    err := csvWriter.Write(r.Fields)
    if err != nil {
      return err
    }

    return nil
  }

  self.Close = func () error {
    csvWriter.Flush()
    file.Close()
    return nil
  }

  return nil
}
