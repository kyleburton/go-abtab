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
		if Verbose {
			fmt.Fprintf(os.Stderr, "TabRecStream: headerProvided=%s\n", source.Header)
		}
	} else {

		if !scanner.Scan() {
			// empty source
			close(out)
			return
		}

		fields := strings.Split(scanner.Text(), source.FieldSeparator)
		source.SetHeader(fields)
		if Verbose {
			fmt.Fprintf(os.Stderr, "TabRecStream: header read from 1st line='%s' header=%s\n", scanner.Text(), source.Header)
		}
	}

	go func() {
		defer file.Close()

		for scanner.Scan() {
			numLines = numLines + 1
			fields := strings.Split(scanner.Text(), source.FieldSeparator)
			numFields := len(source.Header)
			if len(fields) > numFields {
				numFields = len(fields)
			}
			recFields := make([]string, numFields)
			for ii, _ := range fields {
				// turn \N into an empty string for any field where it appears
				// NB: this is a data translation that should be documented!
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

	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"

	delim, delimProvided := qs["delim"]
	if delimProvided {
		self.FieldSeparator = delim[0]
	}

	recSep, recSepProvided := qs["rsep"]
	if recSepProvided {
		self.RecordSeparator = recSep[0]
	}

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

	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"

	delim, delimProvided := qs["delim"]
	if delimProvided {
		self.FieldSeparator = delim[0]
	}

	//fmt.Printf("TabOpenWrite: FieldSeparator=%s\n", self.FieldSeparator)

	recSep, recSepProvided := qs["rsep"]
	if recSepProvided {
		self.RecordSeparator = recSep[0]
	}

	_, noHeader := qs["-header"]
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
