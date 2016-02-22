package abtab

import (
	"fmt"
	"github.com/codegangsta/cli"
)

func Cat(c *cli.Context) {
	fmt.Printf("input=%s\n", c.String("input"))
	fmt.Printf("output=%s\n", c.String("output"))
	inpUrl, err := ParseURL(c.String("input"))
	if err != nil {
		panic(err)
	}

	outUrl, err := ParseURL(c.String("output"))
	if err != nil {
		panic(err)
	}

	err = inpUrl.OpenRead()
	if err != nil {
		panic(err)
	}

	outUrl.Header = inpUrl.Header
	outUrl.OpenWrite()

	var ii int64
	for ii = 0; ii < inpUrl.SkipLines; ii += 1 {
		<-inpUrl.Stream.Recs
	}

	for rec := range inpUrl.Stream.Recs {
		outUrl.WriteRecord(rec)
	}

	outUrl.Close()
}
