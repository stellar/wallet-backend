// Package middleware provides HTTP middleware components for the wallet backend server.
// This file implements GraphQL query complexity logging functionality.
package middleware

import (
	"context"

	"github.com/stellar/go/support/log"
)

// ComplexityLogger implements the gqlgen-complexity-reporter interface
// to log GraphQL query complexity values for monitoring and debugging.
type ComplexityLogger struct{}

// NewComplexityLogger creates a new complexity logger instance.
func NewComplexityLogger() *ComplexityLogger {
	return &ComplexityLogger{}
}

// ReportComplexity logs the complexity of a GraphQL query.
// This method is called by the gqlgen-complexity-reporter extension.
func (c *ComplexityLogger) ReportComplexity(ctx context.Context, operationName string, complexity int) {
	logger := log.Ctx(ctx)

	if operationName == "" {
		operationName = "<unnamed>"
	}

	logger.WithField("operation_name", operationName).
		WithField("complexity", complexity).
		Info("graphql query complexity")
}
