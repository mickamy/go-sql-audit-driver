package formatter

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"time"
)

// SQLValue formats a driver.NamedValue for SQL interpolation.
func SQLValue(arg driver.NamedValue) string {
	switch v := arg.Value.(type) {
	case nil:
		return "NULL"
	case string:
		return fmt.Sprintf("'%s'", escapeString(v))
	case []byte:
		return fmt.Sprintf("'%x'", v)
	case time.Time:
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05-07:00"))
	case fmt.Stringer:
		return fmt.Sprintf("'%s'", escapeString(v.String()))
	default:
		return fmt.Sprintf("'%v'", v)
	}
}

// escapeString escapes single quotes in a string for SQL.
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
