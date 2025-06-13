package audriver

import (
	"path/filepath"
	"strings"
)

// TableFilter is an interface that defines a method to determine if a table should be logged.
type TableFilter interface {
	ShouldLog(tableName string) bool
}

// TableFilterFunc is a function type that implements the TableFilter interface.
type TableFilterFunc func(string) bool

// ShouldLog checks if the table name should be logged based on the filter function.
func (f TableFilterFunc) ShouldLog(tableName string) bool {
	return f(tableName)
}

// NewExcludePatternFilter creates a TableFilter that excludes tables matching any of the provided patterns.
func NewExcludePatternFilter(patterns ...string) TableFilter {
	return TableFilterFunc(func(tableName string) bool {
		for _, pattern := range patterns {
			if matched, _ := filepath.Match(pattern, tableName); matched {
				return false
			}
		}
		return true
	})
}

// NewExcludePrefixFilter creates a TableFilter that excludes tables with names starting with any of the provided prefixes.
func NewExcludePrefixFilter(prefixes ...string) TableFilter {
	return TableFilterFunc(func(tableName string) bool {
		for _, prefix := range prefixes {
			if strings.HasPrefix(tableName, prefix) {
				return false
			}
		}
		return true
	})
}

// NewIncludePatternFilter creates a TableFilter that includes only tables matching any of the provided patterns.
func NewIncludePatternFilter(patterns ...string) TableFilter {
	return TableFilterFunc(func(tableName string) bool {
		for _, pattern := range patterns {
			if matched, _ := filepath.Match(pattern, tableName); matched {
				return true
			}
		}
		return false
	})
}

type TableFilters []TableFilter

func (filters TableFilters) ShouldLog(tableName string) bool {
	for _, filter := range filters {
		if !filter.ShouldLog(tableName) {
			return false
		}
	}
	return true
}
