// GraphQL custom scalars package - implements custom scalar types for gqlgen
// Custom scalars extend GraphQL's built-in types with application-specific data types
// This package handles marshaling between Go types and GraphQL representations
package scalars

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"

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
// GraphQL clients can send integers, floats, numeric strings, or json.Number
// (variables decoded from a JSON request body), so all of these are accepted here.
func UnmarshalUInt32(v any) (uint32, error) {
	switch v := v.(type) {
	case int:
		return int64ToUInt32(int64(v))
	case int32:
		return int64ToUInt32(int64(v))
	case int64:
		return int64ToUInt32(v)
	case json.Number:
		i, err := v.Int64()
		if err != nil {
			return 0, fmt.Errorf("parsing %q as UInt32: %w", v, err)
		}
		return int64ToUInt32(i)
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("%v is not an integral UInt32", v)
		}
		return int64ToUInt32(int64(v))
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parsing %q as UInt32: %w", v, err)
		}
		return int64ToUInt32(i)
	default:
		return 0, fmt.Errorf("%T is not a UInt32", v)
	}
}

// int64ToUInt32 range-checks a signed 64-bit value against the uint32 domain.
func int64ToUInt32(i int64) (uint32, error) {
	if i < 0 || i > math.MaxUint32 {
		return 0, fmt.Errorf("%d is out of range for UInt32 [0, %d]", i, uint32(math.MaxUint32))
	}
	return uint32(i), nil
}
