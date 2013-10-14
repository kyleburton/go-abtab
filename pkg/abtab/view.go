package abtab

import (
  "fmt"
)

func init () {
}

func AbtabView (args []string) {
  inpUrl := CmdlineOpts["input"].(*AbtabURL)

  err := inpUrl.OpenRead()
  if err != nil {
    panic(err)
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
  for rec := range inpUrl.Stream.Recs {
    recNum += 1
    fmt.Printf("Record[%d] # %s\n", recNum, inpUrl.OriginalUrl)
    for idx, value := range rec.Fields {
      fmt.Printf("[% 2d] % *s : %s\n", 1+idx, -1*maxFieldWidth, inpUrl.Header[idx], value)
    }
    fmt.Printf("\n")
  }

}

