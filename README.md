# audriver - SQL Audit Driver for Go

audriver is a transparent SQL audit logging driver for Go that automatically captures and logs database modifications (
INSERT, UPDATE, DELETE operations) without requiring changes to your existing application code.

## Features

- **Transparent Auditing**: Works as a wrapper around any standard `sql.Driver` implementation
- **Minimal Code Changes**: Drop-in replacement for existing database drivers
- **Transaction Support**: Properly handles both direct execution and transactional operations
- **Customizable**: Configurable ID generators and table filters
- **Thread-Safe**: Supports concurrent database operations
- **PostgreSQL Optimized**: Built-in support for PostgreSQL with parameter interpolation

## Installation

```bash
go get github.com/mickamy/go-sql-audit-driver
```

## Quick Start

### Basic Usage

```go
package main

import (
	"database/sql"
	"context"

	"github.com/lib/pq"
	"github.com/mickamy/go-sql-audit-driver/audriver"
)

func main() {
	// Wrap your existing driver with audriver
	baseDriver := &pq.Driver{}
	auditDriver := audriver.New(baseDriver)

	// Register the audit driver
	sql.Register("audit-postgres", auditDriver)

	// Use it like any other SQL driver
	db, err := sql.Open("audit-postgres", "postgres://user:pass@localhost/dbname")
	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// Set required context values
	ctx := context.Background()
	ctx = audriver.WithOperatorID(ctx, "user-123")
	ctx = audriver.WithExecutionID(ctx, "execution-456")

	// Execute your queries - they will be automatically audited
	_, err = db.ExecContext(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "John Doe", "john@example.com")
	if err != nil {
		panic(err)
	}

	// Check the audit logs
	rows, err := db.Query("SELECT * FROM database_modifications ORDER BY modified_at DESC LIMIT 1")
	// ... handle results
}
```

## Configuration

### Custom Options

audriver supports various configuration options:

```go
auditDriver := audriver.New(
    baseDriver,
    audriver.WithLogger(customLogger),
    audriver.WithIDGenerator(customIDGenerator),
    audriver.WithOperatorIDExtractor(customOperatorExtractor),
    audriver.WithExecutionIDExtractor(customExecutionExtractor),
    audriver.WithTableFilters(filters...),
)
```

### Custom Logger

```go
type CustomLogger struct{}
func (l *CustomLogger) Log(ctx context.Context, mod audriver.DatabaseModification) {
    log.Printf("Audit Log: %s %s %s %s", mod.OperatorID, mod.TableName, mod.Action, mod.SQL)
}
auditDriver := audriver.New(
	baseDriver,
	audriver.WithLogger(&CustomLogger{}),
)
```

### Custom ID Generator

```go
type CustomIDGenerator struct{}

func (g *CustomIDGenerator) GenerateID() string {
    return "custom-" + uuid.New().String()
}

auditDriver := audriver.New(
	baseDriver,
    audriver.WithIDGenerator(&CustomIDGenerator{}),
)
```

### Custom Context Extractors

```go
customOperatorExtractor := audriver.OperatorIDExtractorFunc(func (ctx context.Context) (string, error) {
    if userID := ctx.Value("current_user_id"); userID != nil {
        return userID.(string), nil
    }
    return "", fmt.Errorf("user ID not found in context")
})

auditDriver := audriver.New(
	baseDriver,
	audriver.WithOperatorIDExtractor(customOperatorExtractor),
)
```

### Table Filtering

```go
// Exclude temporary and log tables
excludeFilter := audriver.NewExcludePrefixFilter("temp_", "log_")

// Include only specific tables
includeFilter := audriver.NewIncludePatternFilter("users", "orders", "products")

auditDriver := audriver.New(
	baseDriver, 
	audriver.WithTableFilters(excludeFilter, includeFilter),
)
```

## Database Schema

audriver requires a `database_modifications` table to store audit logs:

```sql
CREATE TABLE database_modifications
(
    id           UUID PRIMARY KEY,
    operator_id  UUID        NOT NULL,
    execution_id UUID        NOT NULL,
    table_name   VARCHAR(64) NOT NULL,
    action       VARCHAR(10) NOT NULL,
    sql          TEXT        NOT NULL,
    modified_at  TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Recommended indexes
CREATE INDEX idx_database_modifications_operator_id ON database_modifications (operator_id);
CREATE INDEX idx_database_modifications_execution_id ON database_modifications (execution_id);
CREATE INDEX idx_database_modifications_table_name ON database_modifications (table_name);
CREATE INDEX idx_database_modifications_modified_at ON database_modifications (modified_at);
```

## Audit Log Structure

Each audit log entry contains:

- **id**: Unique identifier for the audit record
- **operator_id**: ID of the user/system performing the operation
- **execution_id**: Unique identifier for the execution context
- **table_name**: Name of the table being modified
- **action**: Type of operation (`insert`, `update`, `delete`)
- **sql**: The actual SQL statement with interpolated parameters
- **modified_at**: Timestamp when the operation occurred

## Context Requirements

audriver requires two context values to be set:

```go
ctx = audriver.WithOperatorID(ctx, "user-or-system-id")
ctx = audriver.WithExecutionID(ctx, "unique-execution-id")
```

These can also be retrieved:

```go
operatorID, err := audriver.GetOperatorID(ctx)
executionID, err := audriver.GetExecutionID(ctx)
```

## Transaction Behavior

- **Direct Execution**: Audit logs are written immediately when operations are executed
- **Transactions**: Audit logs are buffered and written as a batch when the transaction commits
- **Rollbacks**: Buffered audit logs are discarded when transactions are rolled back

## Supported Operations

audriver automatically captures:

- ✅ INSERT statements
- ✅ UPDATE statements
- ✅ DELETE statements
- ❌ SELECT statements (read operations are not audited)
- ❌ DDL operations (CREATE, ALTER, DROP tables, etc.)

## Performance Considerations

- Audit logging adds minimal overhead to database operations
- Batch logging is used for transactions to reduce I/O
- Use table filters to exclude frequently modified temporary tables

## Testing

Run the test suite:

```bash
make up-d
make test
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Roadmap

- [ ] Support for MySQL
- [ ] Support for SQLite
- [ ] Column-level filtering in audit logs
- [ ] Performance optimizations for high-concurrency scenarios

## FAQ

### Q: Does audriver affect the performance of my application?

A: audriver adds minimal overhead. Audit logs are written efficiently, and batch operations are used for transactions.
However, like any logging system, there may be a performance impact depending on the volume of database operations.

### Q: Can I use audriver with existing applications?

A: Yes! audriver is designed as a drop-in replacement that requires minimal changes to your existing application code.

### Q: What happens if the audit table is unavailable?

A: Database operations will fail if audit logging fails, ensuring data consistency between your application data and
audit logs.

### Q: Can I audit only specific tables?

A: Yes, use table filters to include or exclude specific tables from auditing.
