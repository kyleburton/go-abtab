package abtab

import (
  //"fmt"
  "strconv"
  "strings"
  "go/token"
  "github.com/kyleburton/go-eval"
)

func init () {
}

func libParseInt (th *eval.Thread, in []eval.Value, out []eval.Value) {
  res, err := strconv.ParseInt(in[0].String(), 10, 64)
  if err != nil {
    panic(err)
  }
  out[0] = eval.ToValue(int(res))
}

func libParseFloat (th *eval.Thread, in []eval.Value, out []eval.Value) {
  res, err := strconv.ParseFloat(in[0].String(), 64)
  if err != nil {
    panic(err)
  }
  out[0] = eval.ToValue(float64(res))
}

func libSubstr (th *eval.Thread, in []eval.Value, out []eval.Value) {
  s := in[0].String()
  start, err := strconv.ParseInt(in[1].String(), 10, 32)
  var res string
  if err != nil {
    panic(err)
  }
  end, err   := strconv.ParseInt(in[2].String(), 10, 32)
  if err != nil {
    panic(err)
  }

  if int(start) < 0 {
    newStart := len(s) + int(start)
    res := s[newStart:len(s)]
    out[0] = eval.ToValue(res)
    return
  }

  if len(s) < int(end) {
    res = s[start:]
    out[0] = eval.ToValue(res)
    return
  }

  res = s[start:end]
  out[0] = eval.ToValue(res)
}

type LibEntry struct {
  Name   string
  Fn     func(*eval.Thread, []eval.Value, []eval.Value)
  Type   *eval.FuncType
}

var StandardLibrary []LibEntry = []LibEntry {
  {Name: "ParseInt", Fn: libParseInt, 
   Type: eval.NewFuncType( []eval.Type { eval.StringType, },
                                         false,
                                         []eval.Type { eval.Int64Type, })},
  {Name: "ParseFloat", Fn: libParseFloat, 
   Type: eval.NewFuncType( []eval.Type { eval.StringType, },
                                         false,
                                         []eval.Type { eval.Float64Type, })},
  {Name: "Substr", Fn: libSubstr, 
   Type: eval.NewFuncType( []eval.Type { eval.StringType, eval.IntType, eval.IntType },
                                         false,
                                         []eval.Type { eval.StringType, })},
}

func InstallStandardLibrary (w *eval.World) {
  for _, lib := range StandardLibrary {
    fv := eval.FuncFromNative(lib.Fn, lib.Type)
    w.DefineVar(lib.Name, lib.Type, fv)
  }
}

func ScrubFieldNameForEval (s string) string {
  s = strings.Replace(s, " ", "_", -1)
  s = strings.Replace(s, "-", "_", -1)
  return s
}

func AbtabGrepMakeExpressionFilter (inp *AbtabURL, expr string) AbtabFilterFn {
  var fset = token.NewFileSet()
  w := eval.NewWorld()
  InstallStandardLibrary(w)

  var vars map[string]*eval.Variable = make(map[string]*eval.Variable)

  var empty = ""
  for _, fname := range inp.Header {
    v, _ := w.DefineVar(ScrubFieldNameForEval(fname), eval.TypeOfNative(empty), eval.ToValue(empty))
    vars[fname] = v
  }

  return func (rec *Rec) (bool, error) {
    // NB: this seems expensive, can we just re-assign to the internal var ref?
    for idx, fname := range inp.Header {
      var value = rec.Fields[idx]
      vars[fname].Init = eval.ToValue(value)
    }

    // NB: this seems expensive, can we compile once?
    code, err := w.Compile(fset, expr)
    if err != nil {
      return false, err
    }

    v, err := code.Run()
    if err != nil {
      return false, err
    }

    if v.String() == "true" {
      return true, nil
    }

    return false, nil
  }
}


func AbtabGrep (args []string) {
  inpUrl     := CmdlineOpts["input"].(*AbtabURL)
  outUrl     := CmdlineOpts["output"].(*AbtabURL)
  expression := CmdlineOpts["expression"].(string)

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

  filterPred := AbtabGrepMakeExpressionFilter(inpUrl, expression)

  for rec := range inpUrl.Stream.Recs {
    keep, err := filterPred(rec)

    if err != nil {
      panic(err)
    }

    if keep {
      outUrl.WriteRecord(rec)
    }
  }
}


