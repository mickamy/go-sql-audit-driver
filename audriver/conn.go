package audriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

type Conn struct {
	driver.Conn
	builder *databaseModificationBuilder
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	conn, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("connection does not support BeginTx")
	}

	buf := &buffer{}

	tx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &loggingTx{
		Tx: tx,
		conn: &txConn{
			Conn:    c.Conn,
			buf:     buf,
			builder: c.builder,
		},
		buf: buf,
	}, nil
}

// ExecContext implements the ExecContext method for the audit connection.
// It logs database modifications if the SQL statement is a modifying statement.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execCtx, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("connection does not support ExecContext")
	}

	// modifying SQL statements outside of transactions are logged directly
	op, err := c.builder.build(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to build database modification: %w", err)
	}
	if op != nil {
		if err := c.logModification(ctx, *op); err != nil {
			return nil, fmt.Errorf("failed to log database modification: %w", err)
		}
	}

	return execCtx.ExecContext(ctx, query, args)
}

// logModification inserts a single database modification commitLogs directly into the database.
func (c *Conn) logModification(ctx context.Context, op DatabaseModification) error {
	execCtx, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return errors.New("connection does not support ExecContext for direct logging")
	}

	_, err := execCtx.ExecContext(
		ctx,
		`INSERT INTO database_modifications (id, operator_id, execution_id, table_name, action, sql, modified_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		[]driver.NamedValue{
			{Name: "id", Value: op.ID},
			{Name: "operator_id", Value: op.OperatorID},
			{Name: "execution_id", Value: op.ExecutionID},
			{Name: "table_name", Value: op.TableName},
			{Name: "action", Value: op.Action.String()},
			{Name: "sql", Value: op.SQL},
			{Name: "modified_at", Value: op.ModifiedAt},
		},
	)

	return err
}

// txConn is a wrapper around driver.Conn that provides transaction support and logs database modifications.
// It implements the driver.Conn interface and provides methods for executing SQL statements within a transaction.
type txConn struct {
	driver.Conn
	buf     *buffer
	builder *databaseModificationBuilder
}

// ExecContext executes SQL statements within a transaction.
// It builds a DatabaseModification from the SQL statement and arguments, and buffers it for later logging.
func (tc *txConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execCtx, ok := tc.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("connection does not support ExecContext")
	}

	op, err := tc.builder.build(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to build database modification: %w", err)
	}

	if op != nil {
		tc.buf.add(*op)
	}

	return execCtx.ExecContext(ctx, query, args)
}

func (tc *txConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	queryCtx, ok := tc.Conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("connection does not support QueryContext")
	}
	return queryCtx.QueryContext(ctx, query, args)
}

// PrepareContext prepares statements within a transaction.
func (tc *txConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	prepareCtx, ok := tc.Conn.(driver.ConnPrepareContext)
	if !ok {
		return nil, errors.New("connection does not support PrepareContext")
	}
	return prepareCtx.PrepareContext(ctx, query)
}

// loggingTx is a wrapper around driver.Tx that logs database modifications within a transaction.
type loggingTx struct {
	driver.Tx
	conn *txConn
	buf  *buffer
}

// Commit commits the transaction and flushes any buffered logs to the database.
func (tx *loggingTx) Commit() error {
	modifications := tx.buf.drain()
	if len(modifications) > 0 {
		if err := tx.log(modifications); err != nil {
			// roll back the transaction if flushing logs fails
			rollbackErr := tx.Rollback()
			if rollbackErr != nil {
				return fmt.Errorf("failed to rollback transaction after logging error: %w: %w", rollbackErr, err)
			}
			return fmt.Errorf("failed to flush logs in transaction: %w", err)
		}
	}

	return tx.Tx.Commit()
}

// Rollback rolls back the transaction and flushes the buffer.
func (tx *loggingTx) Rollback() error {
	_ = tx.buf.drain()
	return tx.Tx.Rollback()
}

// log inserts all buffered database modifications in a single batch operation.
func (tx *loggingTx) log(modifications []DatabaseModification) error {
	if len(modifications) == 0 {
		return nil
	}

	execCtx, ok := tx.Tx.(driver.ExecerContext)
	if !ok {
		return errors.New("transaction does not support ExecContext for logging")
	}

	valuesClauses := make([]string, len(modifications))
	args := make([]driver.NamedValue, 0, len(modifications)*7)

	for i, op := range modifications {
		baseIndex := i * 6
		valuesClauses[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIndex+1, baseIndex+2, baseIndex+3, baseIndex+4, baseIndex+5, baseIndex+6, baseIndex+7)

		args = append(args,
			driver.NamedValue{Ordinal: baseIndex + 1, Value: op.ID},
			driver.NamedValue{Ordinal: baseIndex + 2, Value: op.OperatorID},
			driver.NamedValue{Ordinal: baseIndex + 3, Value: op.ExecutionID},
			driver.NamedValue{Ordinal: baseIndex + 4, Value: op.TableName},
			driver.NamedValue{Ordinal: baseIndex + 5, Value: op.Action.String()},
			driver.NamedValue{Ordinal: baseIndex + 6, Value: op.SQL},
			driver.NamedValue{Ordinal: baseIndex + 7, Value: op.ModifiedAt},
		)
	}

	query := fmt.Sprintf(
		`INSERT INTO database_modifications (id, operator_id, execution_id, table_name, action, sql, modified_at) VALUES %s`,
		strings.Join(valuesClauses, ", "),
	)

	_, err := execCtx.ExecContext(context.Background(), query, args)
	if err != nil {
		return fmt.Errorf("failed to batch commitLogs database modifications: %w", err)
	}

	return nil
}

var (
	_ driver.Conn          = (*Conn)(nil)
	_ driver.ConnBeginTx   = (*Conn)(nil)
	_ driver.ExecerContext = (*Conn)(nil)

	_ driver.ConnPrepareContext = (*txConn)(nil)
	_ driver.ExecerContext      = (*txConn)(nil)
	_ driver.QueryerContext     = (*txConn)(nil)

	_ driver.Tx = (*loggingTx)(nil)
)
