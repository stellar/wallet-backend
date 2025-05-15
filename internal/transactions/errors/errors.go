package errors

import (
	"errors"
)

var ErrOriginalXDRMalformed = errors.New("transaction string (XDR) is malformed")
