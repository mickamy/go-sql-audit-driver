package audriver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"

	"github.com/google/uuid"

	"github.com/mickamy/audriver/internal/postgres"
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
			operatorID, ok := ctx.Value("operator_id").(string)
			if !ok || operatorID == "" {
				return "", fmt.Errorf("operator ID not found in context")
			}
			return operatorID, nil
		})
	}
	if b.executionIDExtractor == nil {
		b.executionIDExtractor = ExecutionIDExtractorFunc(func(ctx context.Context) (string, error) {
			executionID, ok := ctx.Value("execution_id").(string)
			if !ok || executionID == "" {
				return "", fmt.Errorf("execution ID not found in context")
			}
			return executionID, nil
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
	}, nil
}

func (b *databaseModificationBuilder) isFiltered(tableName string) bool {
	return b.tableFilters.ShouldLog(tableName)
}

var (
	dmlRegexp = regexp.MustCompile(`(?i)^\s*(INSERT|UPDATE|DELETE)\b`)
)

func isDML(sql string) bool {
	return dmlRegexp.MatchString(sql)
}
