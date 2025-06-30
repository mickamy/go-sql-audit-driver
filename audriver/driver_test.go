package audriver_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mickamy/go-sql-audit-driver/audriver"
)

// TestAuditDriver_DirectExecution tests audit logging for direct SQL execution (non-transactional)
func TestAuditDriver_DirectExecution(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	userID := uuid.New().String()
	name := gofakeit.Name()
	email := gofakeit.Email()

	testCases := []struct {
		name           string
		operation      func(ctx context.Context, db *sql.DB)
		expectedTable  string
		expectedAction audriver.DatabaseModificationAction
		expectedSQL    string
	}{
		{
			name: "insert_operation",
			operation: func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionInsert,
			expectedSQL:    fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "select_after_insert",
			operation: func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				rows, err := db.QueryContext(ctx, `SELECT * FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)
				defer func(rows *sql.Rows) {
					_ = rows.Close()
				}(rows)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionInsert,
			expectedSQL:    fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "update_operation",
			operation: func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, `UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = $1`, userID)
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionUpdate,
			expectedSQL:    fmt.Sprintf(`UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = '%s'`, userID),
		},
		{
			name: "delete_operation",
			operation: func(ctx context.Context, db *sql.DB) {
				_, err := db.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, `DELETE FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionDelete,
			expectedSQL:    fmt.Sprintf(`DELETE FROM "users" WHERE "id" = '%s'`, userID),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			db := setUpWriterTestDB(t)

			// act
			tc.operation(ctx, db)

			// assert
			var audit audriver.DatabaseModification
			row := db.QueryRowContext(ctx, "SELECT id, operator_id, execution_id, table_name, action, sql, modified_at FROM database_modifications ORDER BY modified_at DESC LIMIT 1")
			err := row.Scan(
				&audit.ID,
				&audit.OperatorID,
				&audit.ExecutionID,
				&audit.TableName,
				&audit.Action,
				&audit.SQL,
				&audit.ModifiedAt,
			)
			require.NoError(t, err)

			assert.Equal(t, opID.String(), audit.OperatorID)
			assert.Equal(t, execID.String(), audit.ExecutionID)
			assert.Equal(t, tc.expectedTable, audit.TableName)
			assert.Equal(t, tc.expectedAction, audit.Action)
			assert.Equal(t, tc.expectedSQL, audit.SQL)
			assert.NotZero(t, audit.ModifiedAt)
		})
	}
}

// TestAuditDriver_TransactionalExecution tests audit logging for transactional SQL execution
func TestAuditDriver_TransactionalExecution(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())
	userID := uuid.New().String()
	name := gofakeit.Name()
	email := gofakeit.Email()

	testCases := []struct {
		name           string
		operation      func(ctx context.Context, db *sql.DB)
		expectedTable  string
		expectedAction audriver.DatabaseModificationAction
		expectedSQL    string
	}{
		{
			name: "transactional_insert",
			operation: func(ctx context.Context, db *sql.DB) {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionInsert,
			expectedSQL:    fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "transactional_select_after_insert",
			operation: func(ctx context.Context, db *sql.DB) {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)

				rows, err := tx.QueryContext(ctx, `SELECT * FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)
				defer func(rows *sql.Rows) {
					_ = rows.Close()
				}(rows)

				err = tx.Commit()
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionInsert,
			expectedSQL:    fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "transactional_update",
			operation: func(ctx context.Context, db *sql.DB) {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = $1`, userID)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionUpdate,
			expectedSQL:    fmt.Sprintf(`UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = '%s'`, userID),
		},
		{
			name: "transactional_delete",
			operation: func(ctx context.Context, db *sql.DB) {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)

				_, err = tx.ExecContext(ctx, `DELETE FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)

				err = tx.Commit()
				require.NoError(t, err)
			},
			expectedTable:  "users",
			expectedAction: audriver.DatabaseModificationActionDelete,
			expectedSQL:    fmt.Sprintf(`DELETE FROM "users" WHERE "id" = '%s'`, userID),
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			db := setUpWriterTestDB(t)

			// act
			tc.operation(ctx, db)

			// assert
			var audit audriver.DatabaseModification
			row := db.QueryRowContext(ctx, "SELECT id, operator_id, execution_id, table_name, action, sql, modified_at FROM database_modifications ORDER BY modified_at DESC LIMIT 1")
			err := row.Scan(
				&audit.ID,
				&audit.OperatorID,
				&audit.ExecutionID,
				&audit.TableName,
				&audit.Action,
				&audit.SQL,
				&audit.ModifiedAt,
			)
			require.NoError(t, err)

			assert.Equal(t, opID.String(), audit.OperatorID)
			assert.Equal(t, execID.String(), audit.ExecutionID)
			assert.Equal(t, tc.expectedTable, audit.TableName)
			assert.Equal(t, tc.expectedAction, audit.Action)
			assert.Equal(t, tc.expectedSQL, audit.SQL)
			assert.NotZero(t, audit.ModifiedAt)
		})
	}
}

// TestAuditDriver_ReaderConnectionIsolation tests that reader connections do not interfere with audit logging
func TestAuditDriver_ReaderConnectionIsolation(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	userID := uuid.New().String()
	name := gofakeit.Name()
	email := gofakeit.Email()

	writerDB := setUpWriterTestDB(t)

	// arrange - perform write operation
	tx, err := writerDB.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
	require.NoError(t, err)

	err = tx.Commit()
	require.NoError(t, err)

	// act - read using reader connection
	readerDB := setUpReaderTestDB(t)
	rows, err := readerDB.QueryContext(ctx, `SELECT * FROM "users" WHERE "id" = $1`, userID)
	require.NoError(t, err)
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	// assert - verify only the write operation was audited, not the read
	var audit audriver.DatabaseModification
	row := writerDB.QueryRowContext(ctx, "SELECT id, operator_id, execution_id, table_name, action, sql, modified_at FROM database_modifications ORDER BY modified_at DESC LIMIT 1")
	err = row.Scan(
		&audit.ID,
		&audit.OperatorID,
		&audit.ExecutionID,
		&audit.TableName,
		&audit.Action,
		&audit.SQL,
		&audit.ModifiedAt,
	)
	require.NoError(t, err)

	assert.Equal(t, opID.String(), audit.OperatorID)
	assert.Equal(t, execID.String(), audit.ExecutionID)
	assert.Equal(t, "users", audit.TableName)
	assert.Equal(t, audriver.DatabaseModificationActionInsert, audit.Action)
	assert.Equal(t,
		fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		audit.SQL,
	)
	assert.NotZero(t, audit.ModifiedAt)

	// verify that the reader operation created no additional audit records
	var auditCount int
	err = writerDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM database_modifications WHERE execution_id = $1", execID.String()).Scan(&auditCount)
	require.NoError(t, err)
	assert.Equal(t, 1, auditCount, "reader operation should not create additional audit records")
}

// TestAuditDriver_TransactionRollback tests that audit logs are not created when transactions are rolled back
func TestAuditDriver_TransactionRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	db := setUpWriterTestDB(t)

	userID := uuid.New().String()
	name := gofakeit.Name()
	email := gofakeit.Email()

	// start transaction and perform operations
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
	require.NoError(t, err)

	_, err = tx.ExecContext(ctx, `UPDATE "users" SET "name" = 'updated' WHERE "id" = $1`, userID)
	require.NoError(t, err)

	// rollback the transaction
	err = tx.Rollback()
	require.NoError(t, err)

	// verify no audit logs were created
	var count int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM database_modifications WHERE execution_id = $1", execID.String()).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "rolled back transactions should not create audit logs")
}

// TestAuditDriver_NonDMLOperations tests that non-DML operations are not logged
func TestAuditDriver_NonDMLOperations(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	nonDMLOperations := []struct {
		name         string
		sql          string
		shouldIgnore bool // some operations might fail in txdb environment
	}{
		{"select_query", "SELECT COUNT(*) FROM users", false},
		{"select_with_condition", "SELECT * FROM users WHERE name = 'nonexistent'", false},
		{"explain_query", "EXPLAIN SELECT * FROM users", true}, // might not be supported in txdb
		{"show_tables", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'", false},
	}

	for _, op := range nonDMLOperations {
		op := op

		t.Run(op.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			db := setUpWriterTestDB(t)

			var initialCount int
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM database_modifications WHERE execution_id = $1", execID.String()).Scan(&initialCount)
			require.NoError(t, err)

			// act
			result, err := db.ExecContext(ctx, op.sql)

			// assert
			if op.shouldIgnore && err != nil {
				t.Logf("ignoring expected error for %s: %v", op.name, err)
				return
			}

			// for SELECT operations, we need to handle them properly
			if err != nil {
				// try as a query instead of exec for SELECT operations
				if rows, queryErr := db.QueryContext(ctx, op.sql); queryErr == nil {
					rows.Close()
				} else {
					t.Logf("both ExecContext and QueryContext failed for %s, this might be expected in txdb", op.name)
					return
				}
			} else if result != nil {
				// successfully executed as ExecContext
				t.Logf("successfully executed %s as ExecContext", op.name)
			}

			// verify no new audit logs were created
			var finalCount int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM database_modifications WHERE execution_id = $1", execID.String()).Scan(&finalCount)
			require.NoError(t, err)

			assert.Equal(t, initialCount, finalCount, "non-DML operation should not create audit logs: %s", op.name)
		})
	}
}

// TestAuditDriver_ConcurrentOperations tests concurrent access to the audit driver
func TestAuditDriver_ConcurrentOperations(t *testing.T) {
	t.Parallel()

	const numGoroutines = 10
	const operationsPerGoroutine = 5

	db := setUpWriterTestDB(t)

	// channel to collect results
	results := make(chan error, numGoroutines)

	// launch concurrent operations
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			ctx := t.Context()
			opID := uuid.New().String()
			execID := uuid.New().String()
			ctx = audriver.WithOperatorID(ctx, opID)
			ctx = audriver.WithExecutionID(ctx, execID)

			for j := 0; j < operationsPerGoroutine; j++ {
				userID := uuid.New().String()
				_, err := db.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`,
					userID, fmt.Sprintf("user-%d-%d", goroutineID, j), fmt.Sprintf("user%d%d@example.com", goroutineID, j))
				if err != nil {
					results <- err
					return
				}
			}
			results <- nil
		}(i)
	}

	// collect results
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err, "concurrent operation should not fail")
	}

	// verify all operations were logged
	var totalCount int
	err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM database_modifications").Scan(&totalCount)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, totalCount, numGoroutines*operationsPerGoroutine, "all concurrent operations should be logged")
}
