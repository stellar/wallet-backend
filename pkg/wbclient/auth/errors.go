package auth

import (
	"fmt"
	"time"
)

type ExpiredTokenError struct {
	ExpiredBy time.Duration
	InnerErr  error
}

func (e *ExpiredTokenError) Error() string {
	return fmt.Sprintf("the JWT token has expired by %s: %v", e.ExpiredBy, e.InnerErr)
}

func (e *ExpiredTokenError) Unwrap() error {
	return e.InnerErr
}
