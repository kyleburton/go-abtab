package abtab

func AbtabTail(inpUrl, outUrl *AbtabURL, numLines int64) error {
	if err := inpUrl.OpenRead(); err != nil {
		return err
	}

	outUrl.Header = inpUrl.Header
	if err := outUrl.OpenWrite(); err != nil {
		return err
	}

	// TODO: make this more efficient:
	//   if inpUrl is stdin, can't mmap, just stream
	//   if inpUrl is a file, use mmap and look backwards for
	//     the newlines, then re-open the stream, seek
	//     forward and stream recs
	// see: http://stackoverflow.com/questions/9203526/mapping-an-array-to-a-file-via-mmap-in-go

	buff := make([]*Rec, 0)

	for rec := range inpUrl.Stream.Recs {
		buff = append(buff, rec)
		if len(buff) > int(numLines) {
			buff = buff[1:]
		}
	}

	for _, rec := range buff {
		if err := outUrl.WriteRecord(rec); err != nil {
			return err
		}
	}

	if err := inpUrl.Close(); err != nil {
		return err
	}
	return outUrl.Close()
}
