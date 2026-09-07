package abtab

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func AbtabSortSort(inp *AbtabURL, tofile string, keys []string, tmpdir string, numeric, reverse bool) (*AbtabURL, error) {
	tmpfile, err := TempFileName(tmpdir)
	if err != nil {
		return nil, err
	}

	// rewrite the input file to tmpfile1 as tab delimited
	// stripping the header (so sort doesn't have to deal with it)
	outpUrl := fmt.Sprintf("tab://%s?-header", tmpfile)
	fmt.Printf("AbtabSortSort: outpUrl=%s\n", outpUrl)
	outp, err := ParseURL(outpUrl)
	if err != nil {
		return nil, err
	}

	inp.OpenRead()

	var ii int64
	for ii = 0; ii < inp.SkipLines; ii += 1 {
		<-inp.Stream.Recs
	}

	outp.SetHeader(append(keys, inp.Header...))
	outp.OpenWrite()

	for rec := range inp.Stream.Recs {
		prefix := make([]string, 0)
		for _, fname := range keys {
			val := rec.Get(fname)
			prefix = append(prefix, val)
		}
		rec.Fields = append(prefix, rec.Fields...)
		outp.WriteRecord(rec)
	}

	inp.Close()
	outp.Close()

	// put the sort keys in the front
	// shell out to sort -T tmpdir -t '\t' -k 1.1,2.2 -o tmpfile2 tmpfile1
	// os.Remove(tmpfile)
	sortCmd := []string{"-T", tmpdir, "-t", "\t", "-o", tofile}

	if numeric {
		sortCmd = append(sortCmd, "-n")
	}

	if reverse {
		sortCmd = append(sortCmd, "-r")
	}

	for idx, _ := range keys {
		sortCmd = append(sortCmd, fmt.Sprintf("-k %d,%d ", 1+idx, 1+idx))
	}

	sortCmd = append(sortCmd, tmpfile)

	fmt.Printf("Shell out: %s\n", strings.Join(sortCmd, " "))
	cmd := exec.Command("sort", sortCmd...)
	cmd.Start()

	return inp, nil
}

func AbtabSort(unsortedInp, outp *AbtabURL, sortKey, tmpdir string, numeric, reverse bool) error {
	tmpfile, err := TempFileName(tmpdir)
	if err != nil {
		return err
	}

	if Verbose {
		fmt.Fprintf(os.Stderr, "AbtabSort: key=%s\n", sortKey)
	}

	err = unsortedInp.OpenRead()
	if err != nil {
		return err
	}

	outp.Header = unsortedInp.Header
	if err := outp.OpenWrite(); err != nil {
		return err
	}

	if err := unsortedInp.Close(); err != nil {
		return err
	}

	sortedInput, err := AbtabSortSort(unsortedInp, tmpfile, strings.Split(sortKey, ","), tmpdir, numeric, reverse)
	if err != nil {
		return err
	}

	var ii int64
	for ii = 0; ii < sortedInput.SkipLines; ii += 1 {
		<-sortedInput.Stream.Recs
	}

	for rec := range sortedInput.Stream.Recs {
		outp.WriteRecord(rec)
	}

	os.Remove(tmpfile)

	return nil
}
