package auth

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/stellar/go/strkey"
)

type customClaims struct {
	BodyHash string `json:"bodyHash"`
	URI      string `json:"uri"`
	jwtgo.RegisteredClaims
}

func (c *customClaims) Validate(audience, uri string, body []byte, maxTimeout time.Duration) error {
	if maxTimeout == 0 {
		maxTimeout = DefaultMaxTimeout
	}

	if c.ExpiresAt == nil {
		return errors.New("the JWT expiration is not set")
	}
	if c.IssuedAt == nil {
		return errors.New("the JWT issuance time is not set")
	}

	if c.ExpiresAt.After(time.Now().Add(maxTimeout)) {
		return fmt.Errorf("the JWT expiration is too long, max timeout is %s", maxTimeout)
	}
	if c.ExpiresAt.Sub(c.IssuedAt.Time) > maxTimeout {
		return fmt.Errorf("the difference between JWT expiration and issuance time is too long, max is %s", maxTimeout)
	}

	if !strkey.IsValidEd25519PublicKey(c.Subject) {
		return errors.New("the JWT subject is not a valid Stellar public key")
	}

	if audience != "" && !slices.Contains(c.Audience, audience) {
		return fmt.Errorf("the JWT audience %s does not match the expected audience [%s]", c.Audience, audience)
	}

	if c.URI != strings.TrimSpace(uri) {
		return fmt.Errorf("the JWT URI %q does not match the expected URI %q", c.URI, uri)
	}

	if c.BodyHash != HashBody(body) {
		return errors.New("the JWT hashed body does not match the expected value")
	}

	return nil
}

// DecodeTokenString parses the token string and populates the customClaims object.
func (c *customClaims) DecodeTokenString(tokenString string) error {
	// Parse the token without verifying the signature
	token, _, err := new(jwtgo.Parser).ParseUnverified(tokenString, c)
	if err != nil {
		return fmt.Errorf("parsing token: %w", err)
	}

	if _, ok := token.Claims.(*customClaims); !ok {
		return fmt.Errorf("invalid claims type")
	}

	return nil
}
