package audriver

import (
	"time"
)

type DatabaseModificationAction string

func (m DatabaseModificationAction) String() string {
	return string(m)
}

const (
	DatabaseModificationActionInsert DatabaseModificationAction = "insert"
	DatabaseModificationActionUpdate DatabaseModificationAction = "update"
	DatabaseModificationActionDelete DatabaseModificationAction = "delete"
)

// DatabaseModification represents a database modification performed by an operator.
type DatabaseModification struct {
	ID string

	// OperatorID is the id of the operator who performed the modification.
	OperatorID string

	// ExecutionID is a unique identifier for the execution that triggered the modification.
	ExecutionID string

	// TableName is the name of the table being modified, e.g., "users", "orders".
	TableName string

	// Action is the type of modification performed, e.g., "create", "update", "delete".
	Action DatabaseModificationAction

	// SQL is the raw SQL query executed for the modification.
	SQL string

	// ModifiedAt is the timestamp when the modification was performed.
	ModifiedAt time.Time
}
