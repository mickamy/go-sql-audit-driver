package audriver

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
)

type Conn struct {
	driver.Conn
	builder  *databaseModificationBuilder
	readOnly bool
	logger   Logger
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	opts.ReadOnly = c.readOnly
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
		_ctx: ctx,
		Tx:   tx,
		conn: &txConn{
			Conn:     c.Conn,
			buf:      buf,
			builder:  c.builder,
			readOnly: c.readOnly,
		},
		buf:    buf,
		logger: c.logger,
	}, nil
}

// ExecContext implements the ExecContext method for the audit connection.
// It logs database modifications if the SQL statement is a modifying statement.
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execCtx, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("connection does not support ExecContext")
	}

	if c.readOnly {
		return execCtx.ExecContext(ctx, query, args)
	}

	// modifying SQL statements outside of transactions are logged directly
	mod, err := c.builder.build(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to build database modification: %w", err)
	}
	if mod != nil {
		if err := c.logModification(ctx, *mod); err != nil {
			return nil, fmt.Errorf("failed to log database modification: %w", err)
		}
	}

	return execCtx.ExecContext(ctx, query, args)
}

// logModification inserts a single database modification directly into the database.
func (c *Conn) logModification(ctx context.Context, mod DatabaseModification) error {
	execCtx, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return errors.New("connection does not support ExecContext for direct logging")
	}

	_, err := execCtx.ExecContext(
		ctx,
		`INSERT INTO database_modifications (id, operator_id, execution_id, table_name, action, sql, modified_at) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		[]driver.NamedValue{
			{Name: "id", Value: mod.ID},
			{Name: "operator_id", Value: mod.OperatorID},
			{Name: "execution_id", Value: mod.ExecutionID},
			{Name: "table_name", Value: mod.TableName},
			{Name: "action", Value: mod.Action.String()},
			{Name: "sql", Value: mod.SQL},
			{Name: "modified_at", Value: mod.ModifiedAt},
		},
	)

	if err != nil {
		c.logger.Log(ctx, mod)
	}

	return err
}

// txConn is a wrapper around driver.Conn that provides transaction support and logs database modifications.
type txConn struct {
	driver.Conn
	buf      *buffer
	builder  *databaseModificationBuilder
	readOnly bool
}

// ExecContext executes SQL statements within a transaction.
// It builds a DatabaseModification from the SQL statement and arguments, and buffers it for later logging.
func (tc *txConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	execCtx, ok := tc.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("connection does not support ExecContext")
	}

	if tc.readOnly {
		return execCtx.ExecContext(ctx, query, args)
	}

	mod, err := tc.builder.build(ctx, query, args)
	if err != nil {
		return nil, fmt.Errorf("failed to build database modification: %w", err)
	}

	res, err := execCtx.ExecContext(ctx, query, args)
	if err != nil {
		return res, err
	}
	if mod != nil {
		tc.buf.add(*mod)
	}

	return res, nil
}

// QueryContext executes read-only queries within a transaction.
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
	_ctx context.Context
	driver.Tx
	conn   *txConn
	buf    *buffer
	logger Logger
}

func (tx *loggingTx) ctx() context.Context {
	if tx._ctx != nil {
		return tx._ctx
	}
	return context.Background()
}

// Commit commits the transaction and flushes any buffered logs to the database.
func (tx *loggingTx) Commit() error {
	modifications := tx.buf.drain()
	ctx := tx.ctx()
	if len(modifications) > 0 {
		if err := tx.log(ctx, modifications); err != nil {
			if rollbackErr := tx.Tx.Rollback(); rollbackErr != nil {
				return fmt.Errorf("failed to rollback after audriver logging error: %v (original error: %w)", rollbackErr, err)
			}
			return fmt.Errorf("failed to flush logs in transaction: %w", err)
		}
	}

	if err := ctx.Err(); err != nil {
		_ = tx.Tx.Rollback()
		return err
	}

	return tx.Tx.Commit()
}

// Rollback rolls back the transaction and drains the buffer.
func (tx *loggingTx) Rollback() error {
	_ = tx.buf.drain()
	return tx.Tx.Rollback()
}

// log inserts all buffered database modifications in a single batch operation.
func (tx *loggingTx) log(ctx context.Context, modifications []DatabaseModification) error {
	if len(modifications) == 0 {
		return nil
	}

	execCtx, ok := tx.Tx.(driver.ExecerContext)
	if !ok {
		return errors.New("transaction does not support ExecContext for logging")
	}

	valuesClauses := make([]string, len(modifications))
	args := make([]driver.NamedValue, 0, len(modifications)*7)

	for i, mod := range modifications {
		baseIndex := i * 7
		valuesClauses[i] = fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIndex+1, baseIndex+2, baseIndex+3, baseIndex+4, baseIndex+5, baseIndex+6, baseIndex+7)
		args = append(args,
			driver.NamedValue{Ordinal: baseIndex + 1, Value: mod.ID},
			driver.NamedValue{Ordinal: baseIndex + 2, Value: mod.OperatorID},
			driver.NamedValue{Ordinal: baseIndex + 3, Value: mod.ExecutionID},
			driver.NamedValue{Ordinal: baseIndex + 4, Value: mod.TableName},
			driver.NamedValue{Ordinal: baseIndex + 5, Value: mod.Action.String()},
			driver.NamedValue{Ordinal: baseIndex + 6, Value: mod.SQL},
			driver.NamedValue{Ordinal: baseIndex + 7, Value: mod.ModifiedAt},
		)
	}

	query := fmt.Sprintf(
		`INSERT INTO database_modifications (id, operator_id, execution_id, table_name, action, sql, modified_at) VALUES %s`,
		strings.Join(valuesClauses, ", "),
	)

	_, err := execCtx.ExecContext(ctx, query, args)
	if err != nil {
		return fmt.Errorf("failed to batch insert database modifications: %w", err)
	}

	for _, mod := range modifications {
		tx.logger.Log(ctx, mod)
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
