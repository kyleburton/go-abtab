package abtab

func AbtabCat(args []string) {
	inpUrl := CmdlineOpts["input"].(*AbtabURL)
	outUrl := CmdlineOpts["output"].(*AbtabURL)

	err := inpUrl.OpenRead()
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
