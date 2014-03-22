package abtab

import (
	"strings"
	//"fmt"
)

func AbtabCut(args []string) {
	inpUrl := CmdlineOpts["input"].(*AbtabURL)
	outUrl := CmdlineOpts["output"].(*AbtabURL)

	err := inpUrl.OpenRead()
	if err != nil {
		panic(err)
	}

	fields := strings.Split(CmdlineOpts["fields"].(string), ",")
	//fmt.Printf("AbtabCut: fields=%s; inpUrl.HeaderMap=%s\n", fields, inpUrl.HeaderMap)

	// cut the header
	newHeader := make([]string, 0)
	fieldIdxs := make([]int, 0)
	for _, fname := range fields {
		fieldIdx := inpUrl.HeaderMap[fname]
		newHeader = append(newHeader, inpUrl.Header[fieldIdx])
		fieldIdxs = append(fieldIdxs, fieldIdx)
	}
	outUrl.Header = newHeader
	outUrl.OpenWrite()
	//fmt.Printf("AbtabCut: newHeader=%s fieldIdxs=%s\n", newHeader, fieldIdxs)

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
