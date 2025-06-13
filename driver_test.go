package audriver_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-txdb"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mickamy/audriver"
)

func init() {
	txdb.Register("txdb", "postgres", writerDSN)
}

func setUpTestDB(t *testing.T) *sql.DB {
	t.Helper()

	driverName := fmt.Sprintf("test_%s_%d", t.Name(), gofakeit.Number(1000, 9999))

	baseDriver := txdb.New("postgres", writerDSN)
	auditDriver := audriver.New(baseDriver)

	sql.Register(driverName, auditDriver)

	db, err := sql.Open(driverName, driverName)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func Test_WithoutTransaction(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	opID := uuid.New()
	execID := uuid.New()
	ctx = audriver.WithOperatorID(ctx, opID.String())
	ctx = audriver.WithExecutionID(ctx, execID.String())

	userID := uuid.New().String()
	name := gofakeit.Name()
	email := gofakeit.Email()

	tcs := []struct {
		name          string
		operate       func(ctx context.Context, writer *sql.DB)
		wantTableName string
		wantAction    audriver.DatabaseModificationAction
		wantSQL       string
	}{
		{
			name: "create",
			operate: func(ctx context.Context, writer *sql.DB) {
				_, err := writer.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
			},
			wantTableName: "users",
			wantAction:    audriver.DatabaseModificationActionInsert,
			wantSQL:       fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "select",
			operate: func(ctx context.Context, writer *sql.DB) {
				_, err := writer.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				rows, err := writer.QueryContext(ctx, `SELECT * FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)
				defer func(rows *sql.Rows) {
					_ = rows.Close()
				}(rows)
			},
			wantTableName: "users",
			wantAction:    audriver.DatabaseModificationActionInsert,
			wantSQL:       fmt.Sprintf(`INSERT INTO "users" ("id", "name", "email") VALUES ('%s', '%s', '%s')`, userID, name, email),
		},
		{
			name: "update",
			operate: func(ctx context.Context, writer *sql.DB) {
				_, err := writer.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				_, err = writer.ExecContext(ctx, `UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = $1`, userID)
				require.NoError(t, err)
			},
			wantTableName: "users",
			wantAction:    audriver.DatabaseModificationActionUpdate,
			wantSQL:       fmt.Sprintf(`UPDATE "users" SET "name" = 'updated_test_user' WHERE "id" = '%s'`, userID),
		},
		{
			name: "delete",
			operate: func(ctx context.Context, writer *sql.DB) {
				_, err := writer.ExecContext(ctx, `INSERT INTO "users" ("id", "name", "email") VALUES ($1, $2, $3)`, userID, name, email)
				require.NoError(t, err)
				_, err = writer.ExecContext(ctx, `DELETE FROM "users" WHERE "id" = $1`, userID)
				require.NoError(t, err)
			},
			wantTableName: "users",
			wantAction:    audriver.DatabaseModificationActionDelete,
			wantSQL:       fmt.Sprintf(`DELETE FROM "users" WHERE "id" = '%s'`, userID),
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			db := setUpTestDB(t)

			// act
			tc.operate(ctx, db)

			// assert
			var m audriver.DatabaseModification
			row := db.QueryRowContext(ctx, "SELECT id, operator_id, execution_id, table_name, action, sql, modified_at FROM database_modifications ORDER BY modified_at DESC LIMIT 1")
			err := row.Scan(
				&m.ID,
				&m.OperatorID,
				&m.ExecutionID,
				&m.TableName,
				&m.Action,
				&m.SQL,
				&m.ModifiedAt,
			)
			require.NoError(t, err)

			assert.Equal(t, opID.String(), m.OperatorID)
			assert.Equal(t, execID.String(), m.ExecutionID)
			assert.Equal(t, tc.wantTableName, m.TableName)
			assert.Equal(t, tc.wantAction, m.Action)
			assert.Equal(t, tc.wantSQL, m.SQL)
		})
	}
}
