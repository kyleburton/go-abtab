package cli

import "strings"

// EncodingsHelp documents the record-stream encodings supported via the
// scheme of a -i/-o URI (tab://..., csv://..., ...), for inclusion in each
// command's --help output.
const EncodingsHelp = `Supported encodings (the URI scheme used with -i/-o, e.g. csv:///path/to/file):

  tab    Tab separated values (the default; used when no scheme is given).
         Fields are tab-separated, records newline-separated. Embedded tabs,
         newlines and CRs in field values are escaped on write and unescaped
         on read. Query options: delim=CHAR (default tab), rsep=CHAR
         (default newline).
  csv    RFC 4180-ish CSV, via Go's encoding/csv.
  fixed  Fixed-width fields. Requires fields=NAME:START-END,... in the
         query string. Query options: leftpad (pad/trim on the left instead
         of the right), padding=CHAR (default space).
  pg     PostgreSQL table or query, read-only:
         pg://host:port/db/table?user=...&password=...&order=...&limit=...&offset=...

Common query options (tab, csv, fixed): header=F1,F2,... (supply a header
for a headerless source), skipLines=N (skip N leading lines), -header (on
an output URI: suppress writing a header line).`

// WithEncodingsHelp appends EncodingsHelp to a command's one-line
// description, for use with kong.Description.
func WithEncodingsHelp(short string) string {
	return strings.TrimSuffix(short, ".") + ".\n\n" + EncodingsHelp
}
