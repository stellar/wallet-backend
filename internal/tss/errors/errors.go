package errors

import (
	"errors"
)

var (
	OriginalXDRMalformed = errors.New("transaction string is malformed")
)
