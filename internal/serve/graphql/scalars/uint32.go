// GraphQL custom scalars package - implements custom scalar types for gqlgen
// Custom scalars extend GraphQL's built-in types with application-specific data types
// This package handles marshaling between Go types and GraphQL representations
package scalars

import (
	"fmt"
	"io"

	graphql "github.com/99designs/gqlgen/graphql"
)

// MarshalUInt32 converts a Go uint32 to GraphQL output
// This function is called by gqlgen when serializing UInt32 fields to GraphQL responses
// GraphQL doesn't have native uint32 support, so we serialize as string representation
func MarshalUInt32(i uint32) graphql.Marshaler {
	// Return a marshaler that writes the uint32 as a string
	return graphql.WriterFunc(func(w io.Writer) {
		// Convert uint32 to string representation for GraphQL
		_, err := io.WriteString(w, fmt.Sprintf("%d", i))
		if err != nil {
			// Panic on write error - this should not happen in normal operation
			panic(err)
		}
	})
}

// UnmarshalUInt32 converts GraphQL input to Go uint32
// This function is called by gqlgen when parsing UInt32 arguments and input fields
// GraphQL clients can send integers, and we convert them to uint32
func UnmarshalUInt32(v any) (uint32, error) {
	// Handle different input types that GraphQL clients might send
	switch v := v.(type) {
	case int:
		// Convert int to uint32 - this is the most common case
		return uint32(v), nil
	default:
		return 0, fmt.Errorf("%T is not a UInt32", v)
	}
}
