package utils

import (
	"bytes"
	"strings"
)

// SanitizeUTF8 sanitizes a string to comply to the UTF-8 character set and Postgres' code zero byte constraint
func SanitizeUTF8(input string) string {
	// Postgres does not allow code zero bytes on the "text" type and will throw "invalid byte sequence" when encountering one
	// https://www.postgresql.org/docs/13/datatype-character.html
	bs := bytes.ReplaceAll([]byte(input), []byte{0}, []byte{})
	return strings.ToValidUTF8(string(bs), "?")
}
