package postgres

import (
	"database/sql/driver"
	"regexp"
	"strings"

	"github.com/mickamy/go-sql-audit-driver/internal/formatter"
)

var (
	dollarPlaceholderRegexp = regexp.MustCompile(`\$\d+`)
)

// InterpolateSQL replaces PostgreSQL dollar placeholders with actual values.
func InterpolateSQL(query string, args []driver.NamedValue) string {
	matches := dollarPlaceholderRegexp.FindAllStringIndex(query, -1)
	if len(matches) == 0 || len(args) == 0 {
		return query
	}

	var builder strings.Builder
	last := 0
	for i, match := range matches {
		builder.WriteString(query[last:match[0]])
		if i < len(args) {
			builder.WriteString(formatter.SQLValue(args[i]))
		} else {
			builder.WriteString("?")
		}
		last = match[1]
	}
	builder.WriteString(query[last:])

	return builder.String()
}
