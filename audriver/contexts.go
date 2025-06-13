package audriver

import (
	"context"
	"fmt"
)

type operatorIDKey struct{}
type executionIDKey struct{}

func WithOperatorID(ctx context.Context, operatorID string) context.Context {
	return context.WithValue(ctx, operatorIDKey{}, operatorID)
}

func WithExecutionID(ctx context.Context, executionID string) context.Context {
	return context.WithValue(ctx, executionIDKey{}, executionID)
}

func GetOperatorID(ctx context.Context) (string, error) {
	operatorID, ok := ctx.Value(operatorIDKey{}).(string)
	if !ok || operatorID == "" {
		return "", fmt.Errorf("operator ID not found in context")
	}
	return operatorID, nil
}

func GetExecutionID(ctx context.Context) (string, error) {
	executionID, ok := ctx.Value(executionIDKey{}).(string)
	if !ok || executionID == "" {
		return "", fmt.Errorf("execution ID not found in context")
	}
	return executionID, nil
}
