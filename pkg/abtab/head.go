package abtab

import (
	"strconv"
)

func AbtabHead(args []string) {
	inpUrl := CmdlineOpts["input"].(*AbtabURL)
	outUrl := CmdlineOpts["output"].(*AbtabURL)

	err := inpUrl.OpenRead()
	if err != nil {
		panic(err)
	}

	outUrl.Header = inpUrl.Header
	outUrl.OpenWrite()

	numLines, err := strconv.ParseInt(CmdlineOpts["numLines"].(string), 10, 64)
	if err != nil {
		panic(err)
	}

	var ii int64
	for ii = 0; ii < inpUrl.SkipLines; ii += 1 {
		<-inpUrl.Stream.Recs
	}

	for rec := range inpUrl.Stream.Recs {
		if numLines < 1 {
			break
		}
		numLines -= 1
		outUrl.WriteRecord(rec)
	}

	inpUrl.Close()
	outUrl.Close()
}
