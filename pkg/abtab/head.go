package abtab

func AbtabHead(inpUrl, outUrl *AbtabURL, numLines int64) error {
	if err := inpUrl.OpenRead(); err != nil {
		return err
	}

	outUrl.Header = inpUrl.Header
	if err := outUrl.OpenWrite(); err != nil {
		return err
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
		if err := outUrl.WriteRecord(rec); err != nil {
			return err
		}
	}

	if err := inpUrl.Close(); err != nil {
		return err
	}
	return outUrl.Close()
}
