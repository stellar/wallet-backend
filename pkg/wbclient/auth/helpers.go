package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
)

// HashBody returns the SHA-256 hash of the body.
func HashBody(body []byte) string {
	hashedBodyBytes := sha256.Sum256(body)
	return hex.EncodeToString(hashedBodyBytes[:])
}

func ParseExpirationDuration(err error) (time.Duration, bool) {
	var validationErr *jwtgo.ValidationError
	if !errors.As(err, &validationErr) || validationErr.Errors&jwtgo.ValidationErrorExpired == 0 {
		return 0, false
	}

	innerErrMsg := validationErr.Inner.Error()
	if !strings.Contains(innerErrMsg, "token is expired by") {
		return 0, false
	}

	expirationTimeStr := strings.ReplaceAll(innerErrMsg, "token is expired by ", "")
	expirationDuration, err := time.ParseDuration(expirationTimeStr)
	if err != nil {
		return 0, false
	}

	return expirationDuration, true
}
