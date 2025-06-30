package audriver_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-txdb"
	"github.com/brianvoe/gofakeit/v7"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/mickamy/go-sql-audit-driver/audriver"
)

const (
	readerDSN = "user=audriver_reader password=password dbname=audriver host=localhost port=5432 sslmode=disable"
	writerDSN = "user=audriver_writer password=password dbname=audriver host=localhost port=5432 sslmode=disable"
)

func init() {
	txdb.Register("txdb_writer", "postgres", writerDSN)
	txdb.Register("txdb_reader", "postgres", readerDSN)
}

func setUpReaderTestDB(t *testing.T) *sql.DB {
	t.Helper()

	driverName := fmt.Sprintf("reader_test_%s_%d", t.Name(), gofakeit.Number(1000, 9999))

	baseDriver := txdb.New("postgres", readerDSN)
	auditDriver := audriver.New(baseDriver)

	sql.Register(driverName, auditDriver)

	db, err := sql.Open(driverName, driverName)
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func setUpWriterTestDB(t *testing.T) *sql.DB {
	t.Helper()

	driverName := fmt.Sprintf("writer_test_%s_%d", t.Name(), gofakeit.Number(1000, 9999))

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
