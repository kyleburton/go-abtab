package abtab

import (
	"strings"
	"strconv"
	"fmt"
	"os"
)

func AbtabCut(args []string) {
	inpUrl := CmdlineOpts["input"].(*AbtabURL)
	outUrl := CmdlineOpts["output"].(*AbtabURL)

	err := inpUrl.OpenRead()
	if err != nil {
		panic(err)
	}

	fieldSpecs := strings.Split(CmdlineOpts["fields"].(string), ",")
	if Verbose {
		fmt.Fprintf(os.Stderr, "AbtabCut: fields=%s; inpUrl.HeaderMap=%s\n", fieldSpecs, inpUrl.HeaderMap)
	}

	// cut the header
	newHeader := make([]string, 0)
	fieldIdxs := make([]int, 0)
	var fieldIdx int
	var ok bool
	for _, fname := range fieldSpecs {
		fieldIdx, ok = inpUrl.HeaderMap[fname]
		if !ok {
			fieldIdx, err = strconv.Atoi(fname)
			if err != nil {
				panic(fmt.Sprintf("Error[cut]: field='%s' is invalid (does not match a header, does not parse as an int): %s", fname, err))
			}
			// field indexes for humans are 1's based, but
			// 0's based
			fieldIdx = fieldIdx - 1
		}

		// if Verbose {
		// 	fmt.Fprintf(os.Stderr, "AbtabCut: fieldIdx:%d len(inpuUrl.Header)=%d\n", fieldIdx, len(inpUrl.Header[fieldIdx]))
		// }

		newHeader = append(newHeader, inpUrl.Header[fieldIdx])
		fieldIdxs = append(fieldIdxs, fieldIdx)
	}

	outUrl.Header = newHeader
	outUrl.OpenWrite()
	if Verbose {
		fmt.Fprintf(os.Stderr, "AbtabCut: newHeader=%s fieldIdxs=%s\n", newHeader, fieldIdxs)
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
		outUrl.WriteRecord(rec)
	}

	outUrl.Close()
}
