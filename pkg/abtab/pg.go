package abtab

import (
  "os"
  "fmt"
  "strings"
  _ "github.com/lib/pq"
  "database/sql"
)

func PgOptsFromUrl (u *AbtabURL) (map[string]string, error) {
  res := make(map[string] string)

  qs := u.Url.Query()

  username := ""
  password := ""
  hostname := u.Url.Host
  port     := "5432"
  dbname   := "postgres"

  if len(u.Url.Host) > 0 {
    parts := strings.SplitN(u.Url.Host, ":", 2)
    hostname = parts[0]
    if len(parts) > 1 {
      port = parts[1]
    }
  }

  uname, ok := qs["user"]
  if ok {
    username = uname[0]
  }

  p, ok := qs["password"]
  if ok {
    password = p[0]
  }

  /*
  if len(u.Url.User.Username()) > 0 {
    // NB: may need to URI Unescape both user and pass
    username = u.Url.User.Username()
  }

  pass, ok := u.Url.User.Password()
  if ok {
    // NB: may need to URI Unescape both user and pass
    password = pass
  }
  */

  parts := strings.SplitN(u.Url.Path, "/", 4)
  dbname = parts[1]

  if Verbose {
    fmt.Fprintf(os.Stderr, "PgOpenRead: Path.parts=%s\n", parts)
    fmt.Fprintf(os.Stderr, "PgOpenRead: dbname=%s\n", dbname)
  }
  schemaName := "public"
  tableName := parts[2]

  if len(parts) > 3 {
    schemaName = tableName
    tableName = parts[3]
  }

  res["username"]   = username
  res["password"]   = password
  res["hostname"]   = hostname
  res["port"]       = port
  res["dbname"]     = dbname
  res["schemaName"] = schemaName
  res["tableName"]  = tableName

  return res, nil
}

func PgConnect (u *AbtabURL) (*sql.DB, error) {
  opts, err := PgOptsFromUrl(u)
  if err != nil {
    return nil, err
  }

  connectStr := fmt.Sprintf("user=%s password=%s host='%s' port='%s' dbname='%s'",
      opts["username"],
      opts["password"],
      opts["hostname"],
      opts["port"],
      opts["dbname"])

  if Verbose {
    fmt.Fprintf(os.Stderr, "PgOpenRead: connect string: %s\n", connectStr)
  }

  db, err := sql.Open("postgres", connectStr)

  if err != nil {
    return nil, err
  }

  return db, nil
}

func (self *AbtabURL) PgOpenRead () error {
  if Verbose {
    fmt.Fprintf(os.Stderr, "PgOpenRead: %s\n", self)
  }

  self.Stream = &PushBackRecStream{
    Name:     self.OriginalUrl,
    Recs:     make(chan *Rec),
    LastRecs: make([]*Rec, 0),
  }

  opts, err := PgOptsFromUrl(self)
  if err != nil {
    panic(err)
  }

  db, err := PgConnect(self)
  if err != nil {
    panic(err)
  }

  // get the column list
  sqlStmt := fmt.Sprintf("SELECT * FROM %s.%s LIMIT 0", opts["schemaName"], opts["tableName"])
  rows, err := db.Query(sqlStmt)
  if err != nil {
    fmt.Fprintf(os.Stderr, "PgOpenRead: Query Failed: sqlStmt='%s' : error=%h\n", sqlStmt, err)
    panic(err)
  }

  columns, err := rows.Columns()
  if err != nil {
    panic(err)
  }

  numCols := len(columns)
  rows.Close()

  // cast them all to text
  colSpecs := make([]string, numCols)
  for idx, cname := range columns {
    colSpecs[idx] = "coalesce(" + cname + "::text, '')"
  }

  sqlStmt = fmt.Sprintf("SELECT %s FROM %s.%s",
    strings.Join(colSpecs, ", "),
    opts["schemaName"], opts["tableName"])

  qs := self.Url.Query()
  orderBy, hasOrderBy := qs["order"]
  if hasOrderBy {
    sqlStmt = fmt.Sprintf("%s ORDER BY %s", sqlStmt, orderBy[0])
  }

  limit, hasLimit := qs["limit"]
  if hasLimit {
    sqlStmt = fmt.Sprintf("%s LIMIT %s", sqlStmt, limit[0])
  }

  offset, hasOffset := qs["offset"]
  if hasOffset {
    sqlStmt = fmt.Sprintf("%s OFFSET %s", sqlStmt, offset[0])
  }

  if Verbose {
    fmt.Fprintf(os.Stderr, "PgOpenRead: db=%s\n", db)
    fmt.Fprintf(os.Stderr, "PgOpenRead: sqlStmt='%s'\n", sqlStmt)
  }

  rows, err = db.Query(sqlStmt)
  if err != nil {
    fmt.Fprintf(os.Stderr, "PgOpenRead: Query Failed: sqlStmt='%s' : error=%h\n", sqlStmt, err)
    panic(err)
  }

  self.SetHeader(columns)

  go DbRecStream(self, db, numCols, rows)

  self.WriteRecord = func (r *Rec) error {
    return AbtabError{Message: "Error: Pg: not open for writing!"}
  }

  self.Close = func () error {
    return nil
  }

  return nil
}
