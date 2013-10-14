package abtab

import (
  "strconv"
)

func AbtabTail (args []string) {
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
    outUrl.WriteRecord(rec)
  }

  inpUrl.Close()
  outUrl.Close()
}


