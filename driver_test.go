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

	"github.com/mickamy/audriver"
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
	assert.Equal(t, 1, auditCount, "Reader operation should not create additional audit records")
}
