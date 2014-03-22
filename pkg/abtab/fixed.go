package abtab

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TODO: pull field specifications off of the URL:
//   fixed://file.fixed?F1=0:10&F2=11:17&F3=18:20
// parse query string into FixedWidthFieldSpecs / FixedWidthRecordSpec

type FixedWidthFieldSpec struct {
	Name     string
	StartPos int
	EndPos   int
	Width    int
}

func (self *FixedWidthFieldSpec) String() string {
	return fmt.Sprintf("FixedField{%s:%d-%d}", self.Name, self.StartPos, self.EndPos)
}

type FixedWidthRecordSpec struct {
	Fields     []*FixedWidthFieldSpec
	PadChar    string
	PadOnRight bool
}

func (self *FixedWidthRecordSpec) String() string {
	var fields []string
	for _, field := range self.Fields {
		fields = append(fields, field.String())
	}

	return fmt.Sprintf("FixedSpec{%s}", strings.Join(fields, ";"))
}

func FixedWidthRecStream(source *AbtabURL, fname string, out chan *Rec, header []string, headerProvided bool) {
	var file *os.File
	var err error

	if fname == "/dev/stdin" || fname == "//dev/stdin" {
		file = os.Stdin
	} else {
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

	fixedFormatSpec, err := source.FixedWidthParseFieldSpec()

	if headerProvided {
		source.SetHeader(fixedFormatSpec.ParseString(header[0]))
		if Verbose {
			fmt.Fprintf(os.Stderr, "FixedWidthOpenRead: headerProvided=%s\n", source.Header)
		}
	} else {

		if !scanner.Scan() {
			// empty source
			close(out)
			return
		}

		headerLine := scanner.Text()
		headerFields := fixedFormatSpec.ParseString(headerLine)
		source.SetHeader(headerFields)
		if Verbose {
			fmt.Fprintf(os.Stderr, "FixedWidthOpenRead: header read from 1st line='%s' header=%s\n", headerLine, headerFields)
		}
	}

	go func() {
		defer file.Close()

		for scanner.Scan() {
			numLines = numLines + 1
			// fields := strings.Split(scanner.Text(), "\t")
			fields := fixedFormatSpec.ParseString(scanner.Text())
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

func FixedWidthFilePath(u *AbtabURL) string {
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

func (self *AbtabURL) FixedWidthOpenRead() error {
	var fileName = FixedWidthFilePath(self)
	var err error
	self.Stream = &PushBackRecStream{
		Name:     fileName,
		Recs:     make(chan *Rec),
		LastRecs: make([]*Rec, 0),
	}

	qs := self.Url.Query()
	header, headerProvided := qs["header"]

	FixedWidthRecStream(self, fileName, self.Stream.Recs, header, headerProvided)

	skipLines, ok := qs["skipLines"]
	if ok {
		self.SkipLines, err = strconv.ParseInt(skipLines[0], 10, 64)
		if err != nil {
			return AbtabError{Message: fmt.Sprintf("Error parsing skipLines: %s", skipLines[0]), CausedBy: err}
		}
	}

	self.WriteRecord = func(r *Rec) error {
		return AbtabError{Message: "Error: FixedWidth: not open for writing!"}
	}
	self.Close = func() error {
		return nil
	}
	// NB: pull these optionally from the QueryString
	self.FieldSeparator = "\t"
	self.RecordSeparator = "\n"
	return nil
}

func (self *AbtabURL) FixedWidthParseFieldSpec() (spec *FixedWidthRecordSpec, err error) {
	qs := self.Url.Query()

	_, ok := qs["fields"]
	if !ok {
		panic(fmt.Sprintf("FixedWidth: Error: a field specification must be provided!"))
	}

	fields := self.Url.Query()["fields"]

	if Verbose {
		fmt.Fprintf(os.Stderr, "FixedWidthParseFieldSpec: fields=%s\n", fields)
	}

	if len(fields) < 1 {
		panic(fmt.Sprintf("FixedWidth: Error invalid field specification (blank?): '%s'", fields))
	}

	fieldSpecs := strings.Split(fields[0], ",")

	spec = &FixedWidthRecordSpec{
		PadChar:    " ",
		PadOnRight: true,
	}

	for _, fspec := range fieldSpecs {
		parts := strings.SplitN(fspec, ":", 2)
		if len(parts) < 2 {
			panic(fmt.Sprintf("FixedWidth: Error invalid field specification (no start-end positions?): '%s'", fields))
		}

		name := parts[0]
		startAndEnd := strings.SplitN(parts[1], "-", 2)
		if len(startAndEnd) < 2 {
			panic(fmt.Sprintf("FixedWidth: Error invalid field specification (missing start or end position?): '%s'", fields))
		}

		start, err := strconv.Atoi(startAndEnd[0])
		if err != nil {
			panic(fmt.Sprintf("FixedWidth: Error invalid start-position for field=%s start=%s fspec=%s", name, startAndEnd[0], fspec))
		}

		end, err := strconv.Atoi(startAndEnd[1])
		if err != nil {
			panic(fmt.Sprintf("FixedWidth: Error invalid end-position for field=%s start=%s fspec=%s", name, startAndEnd[1], fspec))
		}

		if Verbose {
			fmt.Fprintf(os.Stderr, "FixedWidth: field=%s start=%d end=%d\n", name, start, end)
		}

		if end-start < 1 {
			panic(fmt.Sprintf("FixedWidth: Error field width is zero or negative: %s start=%d end=%d", name, start, end))
		}

		spec.Fields = append(spec.Fields, &FixedWidthFieldSpec{
			Name:     name,
			StartPos: start,
			EndPos:   end,
			Width:    1 + end - start,
		})
	}

	// pad char
	// pad left/right

	if _, ok := qs["leftpad"]; ok {
		spec.PadOnRight = false
	}

	if padChar, ok := qs["padding"]; ok {
		spec.PadChar = padChar[0]
	}

	return spec, nil
}

func (self *FixedWidthRecordSpec) ParseString(s string) (fields []string) {
	for idx, fieldSpec := range self.Fields {
		spos := fieldSpec.StartPos
		epos := fieldSpec.EndPos

		if spos > len(s) {
			fields = append(fields, "")
			continue
		}

		if epos > len(s) {
			epos = len(s) - 1
		}

		val := s[spos:epos]
		// trim the PadChar off of val
		if self.PadOnRight {
			if Verbose {
				fmt.Fprintf(os.Stderr, "Trimming from right: %s sfx=%s\n", val, self.PadChar)
			}
			for len(val) > 0 && strings.HasSuffix(val, self.PadChar) {
				if Verbose {
					fmt.Fprintf(os.Stderr, "Trim Suffix: %s => %s\n", val, val[0:len(val)-1])
				}
				val = val[0 : len(val)-1]
			}
		} else {
			if Verbose {
				fmt.Fprintf(os.Stderr, "Trimming from left: %s sfx=%s\n", val, self.PadChar)
			}

			for len(val) > 0 && strings.HasPrefix(val, self.PadChar) {
				if Verbose {
					fmt.Fprintf(os.Stderr, "Trim Suffix: %s => %s\n", val, val[0:len(val)-1])
				}
				val = val[1:len(val)]
			}

		}

		fields = append(fields, val)

		if Verbose {
			fmt.Fprintf(os.Stderr, "FixedWidthRecordSpec.ParseString: val='%s' idx=%d fieldSpec=%s s.%d='%s'\n",
				val, idx, fieldSpec, s, len(s))
		}

	}

	return
}

func (self *FixedWidthRecordSpec) FormatRec(rec []string) (s string) {
	// abcdefghijklmnopqrstuvwxyz
	// 0123456789012345678901234567890123456789
	//           1         2         3
	var values []string
	for idx, fieldSpec := range self.Fields {
		value := rec[idx]
		if len(value) > fieldSpec.Width {
			value = value[0 : fieldSpec.Width-1]
		}

		for len(value) < fieldSpec.Width {
			if self.PadOnRight {
				value = value + self.PadChar
			} else {
				value = self.PadChar + value
			}
		}
		values = append(values, value)
	}
	return strings.Join(values, "")
}

func (self *AbtabURL) FixedWidthOpenWrite() error {
	var fileName = FixedWidthFilePath(self)
	var file *os.File
	var err error
	var fixedFormatSpec *FixedWidthRecordSpec
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

	if Verbose {
		fmt.Fprintf(os.Stderr, "FixedWidthOpenWrite: self=%s\n", self)
	}

	fixedFormatSpec, err = self.FixedWidthParseFieldSpec()
	if Verbose {
		fmt.Fprintf(os.Stderr, "FixedWidthOpenWrite: fixedFormatSpec=%s\n", fixedFormatSpec)
	}

	qs := self.Url.Query()
	_, noHeader := qs["-header"]

	// NB: pull these optionally from the QueryString
	self.RecordSeparator = "\n"

	// TODO: if there is no header row, we can write it out based on the field specification
	if !noHeader {
		_, err = file.Write([]byte(fixedFormatSpec.FormatRec(self.Header)))
		if err != nil {
			return err
		}
		_, err = file.Write([]byte(self.RecordSeparator))
		if err != nil {
			return err
		}
	}

	self.WriteRecord = func(r *Rec) error {
		_, err := file.Write([]byte(fixedFormatSpec.FormatRec(r.Fields)))
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
