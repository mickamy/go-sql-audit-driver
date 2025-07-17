package audriver

import (
	"context"
)

type Logger interface {
	Log(ctx context.Context, mod DatabaseModification)
}

type noopLogger struct{}

func (l *noopLogger) Log(ctx context.Context, mod DatabaseModification) {
	// No-op logger does nothing
}
