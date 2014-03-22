package abtab

import (
//"fmt"
)

type Rec struct {
	Source  *AbtabURL
	LineNum int64
	Fields  []string
}

func (self *Rec) Get(fname string) string {
	return self.Fields[self.Source.HeaderMap[fname]]
}

type PushBackRecStream struct {
	Name     string
	Recs     chan *Rec
	LastRecs []*Rec
}
