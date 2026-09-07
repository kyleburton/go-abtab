package abtab

import (
	"fmt"
	"io"
	"math"
)

func AbtabView(inpUrl *AbtabURL, w io.Writer) error {
	if err := inpUrl.OpenRead(); err != nil {
		return err
	}

	var ii int64
	for ii = 0; ii < inpUrl.SkipLines; ii += 1 {
		<-inpUrl.Stream.Recs
	}

	maxFieldWidth := 0
	for _, h := range inpUrl.Header {
		if maxFieldWidth < len(h) {
			maxFieldWidth = len(h)
		}
	}

	var recNum int64 = 0
	colNumWidth := 1 + int(math.Log10(float64(len(inpUrl.Header))))
	for rec := range inpUrl.Stream.Recs {
		recNum += 1
		fmt.Fprintf(w, "Record[%d] # %s\n", recNum, inpUrl.OriginalUrl)
		for idx, value := range rec.Fields {
			var fname string
			if idx < len(inpUrl.Header) {
				fname = inpUrl.Header[idx]
			} else {
				fname = ""
			}
			fmt.Fprintf(w, "[% *s] % *s : %s\n",
				colNumWidth,
				fmt.Sprintf("%d", 1+idx),
				-1*maxFieldWidth, fname, value)
		}
		fmt.Fprintf(w, "\n")
	}

	return nil
}
