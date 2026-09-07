package abtab

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func AbtabCut(inpUrl, outUrl *AbtabURL, fields string) error {
	if err := inpUrl.OpenRead(); err != nil {
		return err
	}

	fieldSpecs := strings.Split(fields, ",")
	if Verbose {
		fmt.Fprintf(os.Stderr, "AbtabCut: fields=%s; inpUrl.HeaderMap=%v\n", fieldSpecs, inpUrl.HeaderMap)
	}

	// cut the header
	newHeader := make([]string, 0)
	fieldIdxs := make([]int, 0)
	var fieldIdx int
	var ok bool
	var err error
	for _, fname := range fieldSpecs {
		fieldIdx, ok = inpUrl.HeaderMap[fname]
		if !ok {
			fieldIdx, err = strconv.Atoi(fname)
			if err != nil {
				return AbtabError{Message: fmt.Sprintf("Error[cut]: field='%s' is invalid (does not match a header, does not parse as an int): %s", fname, err), CausedBy: err}
			}
			// field indexes for humans are 1's based, but
			// 0's based
			fieldIdx = fieldIdx - 1
		}

		newHeader = append(newHeader, inpUrl.Header[fieldIdx])
		fieldIdxs = append(fieldIdxs, fieldIdx)
	}

	outUrl.Header = newHeader
	if err := outUrl.OpenWrite(); err != nil {
		return err
	}
	if Verbose {
		fmt.Fprintf(os.Stderr, "AbtabCut: newHeader=%s fieldIdxs=%v\n", newHeader, fieldIdxs)
	}

	var ii int64
	for ii = 0; ii < inpUrl.SkipLines; ii += 1 {
		<-inpUrl.Stream.Recs
	}

	for rec := range inpUrl.Stream.Recs {
		newFields := make([]string, 0)
		for _, idx := range fieldIdxs {
			newFields = append(newFields, rec.Fields[idx])
		}
		rec.Fields = newFields
		if err := outUrl.WriteRecord(rec); err != nil {
			return err
		}
	}

	return outUrl.Close()
}
