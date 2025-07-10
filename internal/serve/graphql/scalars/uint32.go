package scalars

import (
	"fmt"
	"io"

	graphql "github.com/99designs/gqlgen/graphql"
)

func MarshalUInt32(i uint32) graphql.Marshaler {
	return graphql.WriterFunc(func(w io.Writer) {
		_, err := io.WriteString(w, fmt.Sprintf("%d", i))
		if err != nil {
			panic(err)
		}
	})
}

func UnmarshalUInt32(v any) (uint32, error) {
	switch v := v.(type) {
	case int:
		return uint32(v), nil
	default:
		return 0, fmt.Errorf("%T is not a UInt32", v)
	}
}
