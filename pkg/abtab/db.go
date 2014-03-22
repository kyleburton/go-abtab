package abtab

import (
	"database/sql"
)

func DbRecStream(u *AbtabURL, db *sql.DB, numCols int, rows *sql.Rows) {
	var numLines int64 = 0
	sqlFields := make([]sql.NullString, numCols)

	for rows.Next() {
		scanArgs := make([]interface{}, numCols)
		for i, _ := range sqlFields {
			scanArgs[i] = &sqlFields[i]
		}

		numLines += 1
		err := rows.Scan(scanArgs...)
		if err != nil {
			panic(err)
		}

		fields := make([]string, numCols)
		for idx, ns := range sqlFields {
			if ns.Valid {
				fields[idx] = ns.String
			} else {
				fields[idx] = ""
			}
		}

		u.Stream.Recs <- &Rec{
			Source:  u,
			LineNum: numLines,
			Fields:  fields,
		}
	}
	rows.Close()
	close(u.Stream.Recs)
	err := db.Close()
	if nil != err {
		panic(err)
	}

}
