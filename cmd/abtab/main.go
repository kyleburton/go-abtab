package main

import (
  "fmt"
  "flag"
  "github.com/kyleburton/go-abtab/pkg/abtab"
  "os"
)

func main () {
  var task, input, output, expression, sortKey, tmpdir, sortNumeric, sortReverse, fields string
  flag.StringVar(&task,        "task",         "cat",               "Task (App) to run.")
  flag.StringVar(&input,       "input",        "tab:///dev/stdin",  "Input Source")
  flag.StringVar(&input,       "i",            "tab:///dev/stdin",  "Input Source")
  flag.StringVar(&output,      "output",       "tab:///dev/stdout", "Output Destination")
  flag.StringVar(&output,      "o",            "tab:///dev/stdout", "Output Destination")
  flag.StringVar(&expression,  "expression",   "true",              "Specify Expression")
  flag.StringVar(&expression,  "e",            "true",              "Specify Expression")

  flag.StringVar(&sortKey,     "key",          "",                  "Sort Key Fields, comma separated")
  flag.StringVar(&sortKey,     "k",            "",                  "Sort Key Fields, comma separated")
  flag.StringVar(&sortNumeric, "numeric-sort", "false",             "Sort according to numerical value.")
  flag.StringVar(&sortNumeric, "n",            "false",             "head,tail: number of lines; sort: Sort according to numerical value.")
  flag.StringVar(&sortReverse, "reverse",      "false",             "Reverse sort order")
  flag.StringVar(&sortReverse, "r",            "false",             "Reverse sort order")

  // re-use of '-n'
  flag.StringVar(&sortNumeric, "numLines",     "10",                "Number of lines (head, tail).")

  flag.StringVar(&tmpdir,      "tmpdir",       "/tmp",              "Directory to use for temporary files.")
  flag.StringVar(&fields,      "fields",       "1",                 "cut: fields")
  flag.StringVar(&fields,      "f",            "1",                 "cut: fields")

  flag.BoolVar(&abtab.Verbose, "v",            false,               "Be verbose (to stderr)")

  flag.Parse()

  var err error
  abtab.CmdlineOpts["input"], err = abtab.ParseURL(input)
  if err != nil {
    panic(err)
  }

  abtab.CmdlineOpts["output"], err = abtab.ParseURL(output)
  if err != nil {
    panic(err)
  }

  abtab.CmdlineOpts["expression"]     = expression
  abtab.CmdlineOpts["sortKey"]        = sortKey
  abtab.CmdlineOpts["sortNumeric"]    = sortNumeric
  abtab.CmdlineOpts["numLines"]       = sortNumeric // re-use of -n
  abtab.CmdlineOpts["sortReverse"]    = sortReverse
  abtab.CmdlineOpts["tmpdir"]         = tmpdir
  abtab.CmdlineOpts["fields"]         = fields

  //fmt.Fprintf(os.Stderr, "CmdlineOpts: %s\n", abtab.CmdlineOpts)

  switch {
  case "cat" == task:
    abtab.AbtabCat(flag.Args())
    break
  case "view" == task:
    abtab.AbtabView(flag.Args())
    break
  case "fill-rates" == task:
    abtab.AbtabFillRates(flag.Args())
    break
  case "grep" == task:
    abtab.AbtabGrep(flag.Args())
    break
  case "sort" == task:
    abtab.AbtabSort(flag.Args())
    break
  case "head" == task:
    abtab.AbtabHead(flag.Args())
    break
  case "tail" == task:
    abtab.AbtabTail(flag.Args())
    break
  case "cut" == task:
    abtab.AbtabCut(flag.Args())
    break
  default:
    fmt.Fprintf(os.Stderr, "Error: unrecognized task: %s\n", task)
    os.Exit(1)
  }

  /**
  runtime.GOMAXPROCS(4);
  */
}

/*


type RecGroup struct {
  Left  []Rec
  Right []Rec
}

func (self RecGroup) String () string {
  return fmt.Sprintf("RecGroup{left.len=%d; right.len=%d}",
    len(self.Left), len(self.Right))
}

func (self *PushBackRecChannel) pushBack (r *Rec) {
  if self.lastRec != nil {
    panic("Whoa, can't push back 2 things, sorry")
  }
  self.lastRec = r
  //fmt.Printf("pushBack: self.lastRec is now: %s\n", self.lastRec.Fields)
}

func (self *PushBackRecChannel) readNextGroup(keyIdx int) ([]Rec, bool) {
  var rec *Rec

  if self.lastRec != nil {
    //fmt.Printf("readNextGroup: self.lastRec was not nil, using: %s\n", self.lastRec.Fields[keyIdx])
    rec = self.lastRec
    self.lastRec = nil
  } else {
    r, ok := <-self.c
    if !ok {
      rec = nil
    } else {
      rec = r
    }
  }

  recs := make([]Rec, 0)

  if rec == nil {
    //fmt.Printf("[%s] readNextGroup: input exhausted\n")
    return recs, false
  }

  recs = append(recs, *rec)

  //fmt.Printf("[%s] readNextGroup: read rec key=%s\n", self.name, recs[0].Fields[keyIdx])

  for ;; {
    r, ok := <-self.c
    if !ok {
      break
    }

    if r.Fields[keyIdx] != recs[0].Fields[keyIdx] {
      //fmt.Printf("[%s] readNextGroup: PUSHBACK :: read next rec key=%s != rec.key=%s\n", self.name, recs[0].Fields[keyIdx], r.Fields[keyIdx])
      self.pushBack(r)
      //fmt.Printf("readNextGroup: self.lastRec is now: %s\n", self.lastRec.Fields[keyIdx])
      break
    }

    //fmt.Printf("[%s] readNextGroup: ACC: read next rec key=%s == rec.key=%s\n", self.name, recs[0].Fields[keyIdx], r.Fields[keyIdx])
    recs = append(recs, *r)
  }

  //fmt.Printf("[%s] readNextGroup: group read, returning %s/%d recs\n", self.name, recs[0].Fields[keyIdx], len(recs))
  return recs, true
}

func JoinRecStreams(input1 chan *Rec, input1Field int, input2 chan *Rec, input2Field int, output chan *RecGroup) {
  var noRecs = make([]Rec, 0)

  left  := &PushBackRecChannel { name: "left",  c: input1, }
  right := &PushBackRecChannel { name: "right", c: input2, }

  var haveLRecs = false
  var haveRRecs = false
  var leftRecs  []Rec
  var rightRecs []Rec

  leftRecs,  haveLRecs = left.readNextGroup(input1Field)
  rightRecs, haveRRecs = right.readNextGroup(input2Field)

  for ;; {

    if !haveLRecs && !haveRRecs  {
      fmt.Printf("JoinRecStreams: both streams exhausted\n")
      break
    }

    // fmt.Printf("JoinRecStreams:\n")
    // fmt.Printf("  left=%d/%s\n",  len(leftRecs),  leftRecs[0].Fields[input1Field])
    // fmt.Printf("  right=%d/%s\n", len(rightRecs), rightRecs[0].Fields[input2Field])

    if haveLRecs && !haveRRecs {
      //fmt.Printf("  group LEFT only (no more right): %s\n",  leftRecs[0].Fields[input1Field])
      output <- &RecGroup {
        Left:  leftRecs,
        Right: noRecs,
      }
      leftRecs, haveLRecs = left.readNextGroup(input1Field)
      continue
    }

    if !haveLRecs && haveRRecs {
      //fmt.Printf("  group RIGHT only (no more right): %s\n",  rightRecs[0].Fields[input1Field])
      output <- &RecGroup {
        Left:  noRecs,
        Right: rightRecs,
      }
      rightRecs, haveRRecs = right.readNextGroup(input2Field)
      continue
    }

    leftKey  := leftRecs[0].Fields[input1Field]
    rightKey := rightRecs[0].Fields[input2Field]

    if leftKey < rightKey {
      //fmt.Printf("  group LEFT only (%s < %s): %s\n",  leftKey, rightKey, leftKey)
      output <- &RecGroup {
        Left:  leftRecs,
        Right: noRecs,
      }
      leftRecs, haveLRecs = left.readNextGroup(input1Field)
      continue
    }

    if rightKey < leftKey {
      //fmt.Printf("  group RIGHT only (%s < %s): %s\n",  rightKey, leftKey, rightKey)
      output <- &RecGroup {
        Left:  noRecs,
        Right: rightRecs,
      }
      rightRecs, haveRRecs = right.readNextGroup(input2Field)
      continue
    }

    //fmt.Printf("  group BOTH: %s\n", leftKey)
    output <- &RecGroup {
      Left:  leftRecs,
      Right: rightRecs,
    }
    leftRecs, haveLRecs = left.readNextGroup(input1Field)
    rightRecs, haveRRecs = right.readNextGroup(input2Field)
  }

  close(output)
}

type Properties map[string] interface{}

type JoinFileSpec struct {
  inputFile    string
  fields       []int64
  recChan      PushBackRecChannel
  group        []Rec
  hasGroup     bool
  isExhausted  bool
}

func (self *JoinFileSpec) String () string {
  return fmt.Sprintf("JoinFileSpec{inputFile:%s, fields:%s}", self.inputFile, self.fields)
}

func (self *JoinFileSpec) exhausted () bool {
  return self.isExhausted
}

func (self *JoinFileSpec) readNextGroup () {
  if self.exhausted() {
    return
  }

  self.group = nil
  self.hasGroup = false

  recs, ok := self.recChan.readNextGroup(int(self.fields[0]))
  if ok {
    self.group = recs
    self.hasGroup = true
    //fmt.Printf("JoinFileSpec.readNextGroup: read and got %d recs\n", len(self.group))
    return
  }

  self.isExhausted = true
}

func (self *JoinFileSpec) getKey () (string, bool) {
  //fmt.Printf("JoinFileSpec.getKey: fields[0]=%d, len(group)=%d\n",
  //  self.fields[0], len(self.group))
  if self.hasGroup {
    return self.group[0].Fields[self.fields[0]], true
  }

  return "", false
}

type JoinSpec struct {
  specs []*JoinFileSpec
}

func (self *JoinSpec) String () string {
  return fmt.Sprintf("JoinSpec{specs:%s}", self.specs)
}

func (self *JoinSpec) readAll () {
  for _, spec := range self.specs {
    spec.readNextGroup()
  }
}

func (self *JoinSpec) exhausted () bool {
  for _, spec := range self.specs {
    if !spec.exhausted() {
      return false
    }
  }
  return true
}

func (self *JoinSpec) nextCluster () (*JoinSpec, bool) {
  if self.exhausted() {
    return nil, false
  }

  //fmt.Printf("nextCluster: len(self.specs)=%d\n", len(self.specs))

  var minKey string
  found := false
  for _, spec := range self.specs {
    if spec.exhausted() {
      //fmt.Printf("spec[%d] is exhausted\n", idx)
      continue
    }

    key, ok := spec.getKey()
    if !ok {
      //fmt.Printf("spec[%d] has no key\n", idx)
      continue
    }

    //fmt.Printf("checking key: %s vs %s\n", key, minKey)

    if !found {
      minKey = key
      found = true
      continue
    }

    if key < minKey {
      minKey = key
    }
  }

  if !found {
    return nil, false
  }

  //fmt.Printf("nextCluster: minKey=%s\n", minKey)
  res := &JoinSpec{}
  for _, spec := range self.specs {
    key, ok := spec.getKey()
    if ok && minKey == key {
      //fmt.Printf("nextCluster: Hit on minkey=%s\n", minKey)
      res.specs = append(res.specs, spec)
    }
  }

  return res, true
}

func parseFields (s string) []int64 {
  parts := strings.Split(s,",")
  res := make([]int64, 0)
  for _, p := range(parts) {
    intval, err := strconv.ParseInt(p, 10, 0)
    if err != nil {
      panic("Error parsing string as integer in field spec!")
    }
    //fmt.Printf("parseFields: part=%s, intval=%d\n", p, intval)
    res = append(res, intval)
  }

  //fmt.Printf("parseFields: parts=%s, res=%s\n", parts, res)
  return res
}

func JoinTabFiles (p *Properties) {
  var props = *p
  args := props["args"].([]string)
  fmt.Printf("JoinTabFiles: props=%q\n", props)
  if len(args) < 1 || 0 != (len(args) % 2) {
    panic("Error: invalid arguments, must be pairs of file-name join-key[,join-key2...]")
  }

  joinSpec := &JoinSpec{}
  for ii := 0; ii < len(args); ii += 2 {
    fileSpec := JoinFileSpec {
      inputFile: args[ii],
      fields:    parseFields(args[1+ii]),
      recChan:   PushBackRecChannel{
        name: args[ii],
        c:    make(chan *Rec),
      },
    }
    go RecStream(args[ii], fileSpec.recChan.c)
    joinSpec.specs = append(joinSpec.specs, &fileSpec)
  }
  fmt.Printf("JoinTabFiles: joinSpec=%q\n", joinSpec)

  joinSpec.readAll()

  output, err := os.Create(props["output"].(string))
  if  err != nil {
    panic(fmt.Sprintf("Error opening output file %s : %s", output, err))
  }
  defer output.Close()

  var clustNum int64 = 0
  var groupNum int64 = 0

  for ;; {
    if joinSpec.exhausted() {
      fmt.Printf("all streams exhausted.")
      break
    }

    cluster, ok := joinSpec.nextCluster()
    if ok {
      groupNum += 1
      key, _ := cluster.specs[0].getKey()

      if len(key) < 1 {
        fmt.Printf("Not emitting cluster with empty join key [%s], groupNum: %d\n", key, groupNum)
        fmt.Printf("\n\n")
        for _, spec := range cluster.specs {
          fmt.Printf("  %s: lnum:%d\n", spec.inputFile, spec.group[0].LineNum)
        }
        fmt.Printf("\n\n")
        cluster.readAll()
        continue
      }

      if 0 == (groupNum % 10000) {
        fmt.Printf("Checked %d/%d clusters (at key: %s)\n", clustNum, groupNum, key)
      }

      //fmt.Printf("read next cluster: %s >> %s\n", key, cluster)
      if len(cluster.specs) > 1 {
        clustNum += 1
        fmt.Printf("EMIT[%d]: %s >> %d\n", clustNum, key, len(cluster.specs))

        output.Write([]byte(fmt.Sprintf("%d", clustNum)))
        output.Write([]byte("\t"))
        var fullRec []string
        for _, spec := range(cluster.specs) {
          fullRec = append(fullRec, spec.group[0].Fields...)
        }
        output.Write([]byte(strings.Join(fullRec, "\t")))
        output.Write([]byte("\n"))
      }
      // read the next set
      cluster.readAll()
      continue
    }
    fmt.Printf("\n")
    break
  }
}

func FillRates (p *Properties) {
  var props = *p
  input := props["input"].(string)

  recs := make(chan *Rec)
  go RecStream(input, recs)

  counts := make(map[int] int64)

  var linesProcessed int64 = 0
  for rec := range recs {
    for idx, val := range rec.Fields {
      fieldCount, ok := counts[idx]
      if !ok {
        fieldCount = 0
      }

      if len(val) > 0 {
        fieldCount += 1
      }
      counts[idx] = fieldCount

    }

    linesProcessed += 1
    if 0 == (linesProcessed % 10000) {
      fmt.Printf(".")
    }
  }

  fmt.Printf("\n")
  fmt.Printf("%d total lines\n", linesProcessed)


  for idx := 0; idx < len(counts); idx += 1 {
    fmt.Printf("% 4d: %d : %3.2f%%\n", idx, counts[idx], 100.0 * (float64(counts[idx]) / float64(linesProcessed)))
  }
}

*/
