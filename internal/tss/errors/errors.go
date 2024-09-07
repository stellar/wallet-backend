package errors

import (
	"errors"
)

var (
	OriginalXdrMalformed = errors.New("transaction string is malformed")
)
