package abtab

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func TabRecStream(source *AbtabURL, fname string, out chan *Rec, header []string, headerProvided bool) {
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
	}

	var numLines int64 = 0
	if !headerProvided {
		numLines = -1
	}

	scanner := bufio.NewScanner(file)

	if headerProvided {
		source.SetHeader(strings.Split(header[0], ","))
		//fmt.Fprintf(os.Stderr, "TabOpenRead: headerProvided=%s\n", source.Header)
	} else {

		if !scanner.Scan() {
			// empty source
			close(out)
			return
		}

		fields := strings.Split(scanner.Text(), "\t")
		source.SetHeader(fields)
		//fmt.Fprintf(os.Stderr, "TabOpenRead: header read from 1st line='%s' header=%s\n", scanner.Text(), source.Header)
	}

	go func() {
		defer file.Close()

		for scanner.Scan() {
			numLines = numLines + 1
			fields := strings.SplitN(scanner.Text(), "\t", len(source.Header))
			// turn \N into an empty string for any field where it appears
			numFields := len(source.Header)
			if 0 == numFields {
				numFields = len(fields)
			}
			recFields := make([]string, numFields)
			//fmt.Fprintf(os.Stderr, "TabRecStream: Rec.LineNum=%s len(fields)=%d len(recFields)=%d len(source.Header)=%d\n", numLines,
			//  len(fields), len(recFields), len(source.Header))
			for ii, _ := range fields {
				if fields[ii] == "\\N" {
					fields[ii] = ""
				}
				recFields[ii] = fields[ii]
			}
			out <- &Rec{
				Source:  source,
				LineNum: numLines,
				Fields:  recFields,
			}
		}

		if err := scanner.Err(); err != nil {
			panic(fmt.Sprintf("Error reading from file '%s' : %s", fname, err))
		}

		close(out)

	}()
}

func TabFilePath(u *AbtabURL) string {
	var fileName = ""
	if len(u.Url.Host) > 0 {
		fileName = u.Url.Host + "/"
	}

	fileName += u.Url.Path
	fileName = strings.Replace(fileName, "//", "/", -1)

	if strings.HasSuffix(fileName, "/") {
		fileName = fileName[0 : len(fileName)-1]
	}

	return fileName
}

func (self *AbtabURL) TabOpenRead() error {
	var fileName = TabFilePath(self)
	var err error
	self.Stream = &PushBackRecStream{
		Name:     fileName,
		Recs:     make(chan *Rec),
		LastRecs: make([]*Rec, 0),
	}

	qs := self.Url.Query()
	header, headerProvided := qs["header"]

	TabRecStream(self, fileName, self.Stream.Recs, header, headerProvided)

	skipLines, ok := qs["skipLines"]
	if ok {
		self.SkipLines, err = strconv.ParseInt(skipLines[0], 10, 64)
		if err != nil {
			return AbtabError{Message: fmt.Sprintf("Error parsing skipLines: %s", skipLines[0]), CausedBy: err}
		}
	}

	self.WriteRecord = func(r *Rec) error {
		return AbtabError{Message: "Error: Tab: not open for writing!"}
	}
	self.Close = func() error {
		return nil
	}
	// NB: pull these optionally from the QueryString
	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"
	return nil
}

func (self *AbtabURL) TabOpenWrite() error {
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

	qs := self.Url.Query()
	_, noHeader := qs["-header"]

	// NB: pull these optionally from the QueryString
	self.FieldSeparator = "\t"
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

	self.WriteRecord = func(r *Rec) error {
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

	self.Close = func() error {
		file.Close()
		return nil
	}

	return nil
}
