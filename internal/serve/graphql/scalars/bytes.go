// GraphQL Bytes scalar - handles base64 encoding/decoding of binary data
// This scalar is used for XDR fields that are stored as BYTEA in the database
package scalars

import (
	"encoding/base64"
	"fmt"
	"io"

	graphql "github.com/99designs/gqlgen/graphql"
)

// MarshalBytes converts []byte to base64-encoded GraphQL string output
// This function is called by gqlgen when serializing Bytes fields to GraphQL responses
func MarshalBytes(b []byte) graphql.Marshaler {
	return graphql.WriterFunc(func(w io.Writer) {
		encoded := base64.StdEncoding.EncodeToString(b)
		_, err := io.WriteString(w, `"`+encoded+`"`)
		if err != nil {
			panic(err)
		}
	})
}

// UnmarshalBytes converts base64 GraphQL string input to []byte
// This function is called by gqlgen when parsing Bytes arguments and input fields
func UnmarshalBytes(v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		decoded, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("decoding base64 string: %w", err)
		}
		return decoded, nil
	case []byte:
		return v, nil
	default:
		return nil, fmt.Errorf("%T is not a valid Bytes type", v)
	}
}
