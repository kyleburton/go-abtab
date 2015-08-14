package abtab

import (
	// "bufio"
  "io"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func CsvRecStream(source *AbtabURL, fname string, out chan *Rec, header []string, headerProvided bool) error {
	var err error
  //fmt.Fprintf(os.Stderr, "CsvRecStream: fname=%s\n", fname)

	csvFile, err := os.Open(fname)

  if err != nil {
    panic(err)
  }

  reader := csv.NewReader(csvFile)

  reader.FieldsPerRecord = -1;

	var idx int64
	//fmt.Printf("CsvRecStream: skipping: %d records\n", source.SkipLines)
	for idx = 0; idx < source.SkipLines; idx += 1 {
		reader.Read()
	}

	if headerProvided {
		source.SetHeader(strings.Split(header[0], ","))
	} else {
    fields, err := reader.Read()

		if err != nil {
			panic(err)
		}
		source.SetHeader(fields)
	}

  //fmt.Fprintf(os.Stderr, "CsvRecStream: header=%s\n", source.Header)

	go func() {
		defer csvFile.Close()
		var numLines int64 = 0
    for ;; {
      fields, err := reader.Read()
      if err == io.EOF {
        break;
      }
			numLines = numLines + 1
      // todo: turn \N into an empty string for any field where it appears
			if err != nil {
				panic(err)
			}

			for len(fields) < len(source.Header) {
				fields = append(fields, "")
			}

			out <- &Rec{
				Source:  source,
				LineNum: numLines,
				Fields:  fields,
			}
		}

		close(out)
	}()
	return nil
}

func (self *AbtabURL) CsvOpenRead() error {
	var fileName = TabFilePath(self)
	var err error
	self.Stream = &PushBackRecStream{
		Name:     fileName,
		Recs:     make(chan *Rec),
		LastRecs: make([]*Rec, 0),
	}
	qs := self.Url.Query()

	skipLines, ok := qs["skipLines"]
	if ok {
		self.SkipLines, err = strconv.ParseInt(skipLines[0], 10, 64)
		if err != nil {
			return AbtabError{Message: fmt.Sprintf("Error parsing skipLines: %s", skipLines[0]), CausedBy: err}
		}
	}

	header, headerProvided := qs["header"]
	CsvRecStream(self, fileName, self.Stream.Recs, header, headerProvided)

	self.WriteRecord = func(r *Rec) error {
		return AbtabError{Message: "Error: Csv: not open for writing!"}
	}

	self.Close = func() error {
		return nil
	}
	// NB: pull these optionally from the QueryString
	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"
	return nil
}

func (self *AbtabURL) CsvOpenWrite() error {
	var fileName = TabFilePath(self)
	var file *os.File
	var err error
	self.Stream = &PushBackRecStream{
		Name:     fileName,
		Recs:     make(chan *Rec),
		LastRecs: make([]*Rec, 0),
	}

	if fileName == "/dev/stdout" || fileName == "//dev/stdout" {
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
	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"

	if !noHeader {
		err = csvWriter.Write(self.Header)
		if err != nil {
			return err
		}
	}

	self.WriteRecord = func(r *Rec) error {
		err := csvWriter.Write(r.Fields)
		if err != nil {
			return err
		}

		return nil
	}

	self.Close = func() error {
		csvWriter.Flush()
		file.Close()
		return nil
	}

	return nil
}
