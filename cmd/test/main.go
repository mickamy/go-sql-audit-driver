package main

import (
	"context"
	"database/sql"

	"github.com/google/uuid"
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
	db, err := sql.Open("audit-postgres", "postgres://audriver_writer:password@localhost/audriver?sslmode=disable")
	if err != nil {
		panic(err)
	}
	defer func(db *sql.DB) {
		_ = db.Close()
	}(db)

	// Set required context values
	ctx := context.Background()
	ctx = audriver.WithOperatorID(ctx, uuid.New().String())
	ctx = audriver.WithExecutionID(ctx, uuid.New().String())

	// Execute your queries - they will be automatically audited
	_, err = db.ExecContext(ctx, "INSERT INTO users (id, name, email) VALUES ($1, $2, $3)", uuid.New().String(), "John Doe", "john@example.com")
	if err != nil {
		panic(err)
	}
}
