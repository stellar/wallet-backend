package utils

import (
	"bytes"
	"reflect"
	"strings"
)

// SanitizeUTF8 sanitizes a string to comply to the UTF-8 character set and Postgres' code zero byte constraint
func SanitizeUTF8(input string) string {
	// Postgres does not allow code zero bytes on the "text" type and will throw "invalid byte sequence" when encountering one
	// https://www.postgresql.org/docs/13/datatype-character.html
	bs := bytes.ReplaceAll([]byte(input), []byte{0}, []byte{})
	return strings.ToValidUTF8(string(bs), "?")
}

// IsEmpty checks if a value is empty.
func IsEmpty[T any](v T) bool {
	return reflect.ValueOf(&v).Elem().IsZero()
}

// UnwrapInterfaceToPointer unwraps an interface to a pointer of the given type.
func UnwrapInterfaceToPointer[T any](i interface{}) *T {
	t, ok := i.(*T)
	if ok {
		return t
	}
	return nil
}

// PointOf returns a pointer to the value
func PointOf[T any](value T) *T {
	return &value
}
