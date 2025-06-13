package audriver

import (
	"fmt"
	"regexp"
)

var (
	insertRegexp = regexp.MustCompile(`(?i)\bINSERT\s+INTO\s+(?:[` + "`" + `"\[]?)([^` + "`" + `"\]\s]+)(?:[` + "`" + `"\]]?)`)
	updateRegexp = regexp.MustCompile(`(?i)\bUPDATE\s+(?:[` + "`" + `"\[]?)([^` + "`" + `"\]\s]+)(?:[` + "`" + `"\]]?)`)
	deleteRegexp = regexp.MustCompile(`(?i)\bDELETE\s+FROM\s+(?:[` + "`" + `"\[]?)([^` + "`" + `"\]\s]+)(?:[` + "`" + `"\]]?)`)
)

// tableAction represents a parsed SQL action and its associated table.
type tableAction struct {
	table  string
	action DatabaseModificationAction
}

// actionAndResourceType extracts the action and resource type from the SQL statement.
func parseTableAction(sql string) (tableAction, error) {
	if match := insertRegexp.FindStringSubmatch(sql); len(match) > 1 {
		return tableAction{match[1], DatabaseModificationActionInsert}, nil
	}
	if match := updateRegexp.FindStringSubmatch(sql); len(match) > 1 {
		return tableAction{match[1], DatabaseModificationActionUpdate}, nil
	}
	if match := deleteRegexp.FindStringSubmatch(sql); len(match) > 1 {
		return tableAction{match[1], DatabaseModificationActionDelete}, nil
	}

	return tableAction{}, fmt.Errorf("could not parse action from SQL: %s", sql)
}
