package abtab

import (
  "fmt"
)

func init () {
}


type fillRateRow struct {
  fieldNum int
  fieldName string
  fillCount string
  fillRate  string
}

func AbtabFillRates (args []string) {
  inpUrl := CmdlineOpts["input"].(*AbtabURL)

  err := inpUrl.OpenRead()
  if err != nil {
    panic(err)
  }

  var ii int64
  for ii = 0; ii < inpUrl.SkipLines; ii += 1 {
    <-inpUrl.Stream.Recs
  }

  var numRecs int64 = 0
  fillRates := make(map[int]int64)

  for idx, _ := range inpUrl.Header {
    fillRates[idx] = 0
  }

  for rec := range inpUrl.Stream.Recs {
    numRecs += 1
    for idx, value := range rec.Fields {
      if len(value) > 0 {
        fillRates[idx] = fillRates[idx] + 1
      }
    }
  }

  maxFieldWidth := 0
  for _, h := range inpUrl.Header {
    if maxFieldWidth < len(h) {
      maxFieldWidth = len(h)
    }
  }

  fillRateRows := make([]fillRateRow, 0)
  for idx, fieldName := range inpUrl.Header {
    fillRateRows = append(fillRateRows, fillRateRow{
      fieldNum:   1+idx,
      fieldName:  fieldName,
      fillCount:  fmt.Sprintf("%d", fillRates[idx]),
      fillRate:   fmt.Sprintf("%3.2f", float64(100) * (float64(fillRates[idx]) / float64(numRecs))),
    })
  }

  fmt.Printf("Fill Rates file: %s, Records: %d\n", inpUrl.OriginalUrl, numRecs)

  maxCountWidth := 0
  maxRateWidth := 0
  for _, row := range fillRateRows {
    if maxCountWidth < len(row.fillCount) {
      maxCountWidth = len(row.fillCount)
    }
    if maxRateWidth < len(row.fillRate) {
      maxRateWidth = len(row.fillRate)
    }
  }

  for _, row := range fillRateRows {
      fmt.Printf("[% 2d] % *s : % *s % *s%%\n",
        row.fieldNum,
        -1*maxFieldWidth,
        row.fieldName,
        maxCountWidth,
        row.fillCount,
        maxRateWidth,
        row.fillRate)
  }
}


