package auth

import (
	"crypto/sha256"
	"encoding/hex"
)

// HashBody returns the SHA-256 hash of the body.
func HashBody(body []byte) string {
	hashedBodyBytes := sha256.Sum256(body)
	return hex.EncodeToString(hashedBodyBytes[:])
}
