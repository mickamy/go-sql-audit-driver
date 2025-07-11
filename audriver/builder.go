package audriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	pg_query "github.com/pganalyze/pg_query_go/v6"

	"github.com/mickamy/go-sql-audit-driver/internal/postgres"
)

// IDGenerator generates unique IDs for database modifications.
type IDGenerator interface {
	GenerateID() string
}

// IDGeneratorFunc is a function type that implements the IDGenerator interface.
type IDGeneratorFunc func() string

func (f IDGeneratorFunc) GenerateID() string {
	return f()
}

// OperatorIDExtractor extracts the operator ID from the context.
type OperatorIDExtractor interface {
	ExtractOperatorID(ctx context.Context) (string, error)
}

// OperatorIDExtractorFunc is a function type that implements the OperatorIDExtractor interface.
type OperatorIDExtractorFunc func(ctx context.Context) (string, error)

func (f OperatorIDExtractorFunc) ExtractOperatorID(ctx context.Context) (string, error) {
	return f(ctx)
}

// ExecutionIDExtractor extracts the execution ID from the context.
type ExecutionIDExtractor interface {
	ExtractExecutionID(ctx context.Context) (string, error)
}

// ExecutionIDExtractorFunc is a function type that implements the ExecutionIDExtractor interface.
type ExecutionIDExtractorFunc func(ctx context.Context) (string, error)

func (f ExecutionIDExtractorFunc) ExtractExecutionID(ctx context.Context) (string, error) {
	return f(ctx)
}

// databaseModificationBuilder builds DatabaseModification instances from SQL statements and arguments.
type databaseModificationBuilder struct {
	idGenerator          IDGenerator
	operatorIDExtractor  OperatorIDExtractor
	executionIDExtractor ExecutionIDExtractor
	tableFilters         TableFilters
}

func (b *databaseModificationBuilder) fillDefaults() {
	if b.idGenerator == nil {
		b.idGenerator = IDGeneratorFunc(func() string {
			return uuid.New().String()
		})
	}
	if b.operatorIDExtractor == nil {
		b.operatorIDExtractor = OperatorIDExtractorFunc(func(ctx context.Context) (string, error) {
			return GetOperatorID(ctx)
		})
	}
	if b.executionIDExtractor == nil {
		b.executionIDExtractor = ExecutionIDExtractorFunc(func(ctx context.Context) (string, error) {
			return GetExecutionID(ctx)
		})
	}
	if b.tableFilters == nil {
		b.tableFilters = []TableFilter{}
	}
}

// build creates a DatabaseModification from the provided SQL statement and arguments.
func (b *databaseModificationBuilder) build(ctx context.Context, sql string, args []driver.NamedValue) (*DatabaseModification, error) {
	if !isDML(sql) {
		return nil, nil
	}

	ta, err := parseTableAction(sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse action and table from SQL: %w", err)
	}

	operatorID, err := b.operatorIDExtractor.ExtractOperatorID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract operator ID: %w", err)
	}

	executionID, err := b.executionIDExtractor.ExtractExecutionID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to extract execution ID: %w", err)
	}

	fullSQL := postgres.InterpolateSQL(sql, args)

	return &DatabaseModification{
		ID:          b.idGenerator.GenerateID(),
		OperatorID:  operatorID,
		ExecutionID: executionID,
		TableName:   ta.table,
		Action:      ta.action,
		SQL:         fullSQL,
		ModifiedAt:  time.Now(),
	}, nil
}

func (b *databaseModificationBuilder) isFiltered(tableName string) bool {
	return b.tableFilters.ShouldLog(tableName)
}

var (
	quickDMLRegexp = regexp.MustCompile(`(?i)^\s*(?:INSERT|UPDATE|DELETE)\b`)
)

func isDML(sql string) bool {
	normalized := stripLeading(sql)
	if normalized == "" {
		return false
	}

	if !quickDMLRegexp.MatchString(normalized) {
		return false
	}

	tree, err := pg_query.Parse(sql)
	if err != nil {
		return false
	}
	if len(tree.Stmts) == 0 {
		return false
	}
	for _, stmt := range tree.Stmts {
		switch stmt.Stmt.Node.(type) {
		case *pg_query.Node_InsertStmt,
			*pg_query.Node_UpdateStmt,
			*pg_query.Node_DeleteStmt:
			return true
		}
	}

	return false
}

// stripLeading removes leading whitespace, comments, and simple WITH-CTE from the SQL string.
func stripLeading(sql string) string {
	s := strings.TrimLeft(sql, " \t\r\n")

	// -- comment
	for strings.HasPrefix(s, "--") {
		if idx := strings.Index(s, "\n"); idx >= 0 {
			s = s[idx+1:]
		} else {
			return ""
		}
		s = strings.TrimLeft(s, " \t\r\n")
	}

	// /* */ comment
	for strings.HasPrefix(s, "/*") {
		if idx := strings.Index(s[2:], "*/"); idx >= 0 {
			s = s[idx+4:]
		} else {
			return ""
		}
		s = strings.TrimLeft(s, " \t\r\n")
	}

	// remove simple WITH-CTE
	withCTE := regexp.MustCompile(`(?is)^\s*WITH\s+[^;]+?\)\s*`)
	s = withCTE.ReplaceAllString(s, "")
	return s
}
