package auth

import (
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
)

func Test_CustomClaims_Validate(t *testing.T) {
	validBody := []byte(`{"foo": "bar"}`)
	invalidBody := []byte(`{"x": "y"}`)
	validURI := "/valid/uri"
	invalidURI := "/invalid/uri"
	validAudience := "test.com"
	invalidAudience := "invalid.test.com"
	validSubject := testKP1.Address()
	invalidSubject := "invalid-public-key"
	validIssuedAt := time.Now()
	validExpiresAt := validIssuedAt.Add(DefaultMaxTimeout - time.Second)
	tooLongExpiresAt := validIssuedAt.Add(DefaultMaxTimeout + time.Second)

	testCases := []struct {
		name            string
		claims          *customClaims
		audience        string
		uri             string
		body            []byte
		maxTimeout      time.Duration
		wantErrContains string
	}{
		{
			name: "🔴expiration_too_long",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(tooLongExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the JWT expiration is too long, max timeout is",
		},
		{
			name: "🔴difference_between_exp_and_iat_too_long",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validExpiresAt.Add(-DefaultMaxTimeout - time.Second)),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the difference between JWT expiration and issuance time is too long, max is",
		},
		{
			name: "🔴invalid_subject",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   invalidSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the JWT subject is not a valid Stellar public key",
		},
		{
			name: "🔴invalid_audience",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{invalidAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the JWT audience [invalid.test.com] does not match the expected audience [test.com]",
		},
		{
			name: "🔴invalid_URI",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      invalidURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the JWT URI \"/invalid/uri\" does not match the expected URI \"/valid/uri\"",
		},
		{
			name: "🔴invalid_body_hash",
			claims: &customClaims{
				BodyHash: HashBody(invalidBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:        validAudience,
			uri:             validURI,
			body:            validBody,
			maxTimeout:      15 * time.Second,
			wantErrContains: "the JWT hashed body does not match the expected value",
		},
		{
			name: "🟢valid_claims",
			claims: &customClaims{
				BodyHash: HashBody(validBody),
				URI:      validURI,
				RegisteredClaims: jwtgo.RegisteredClaims{
					Subject:   validSubject,
					IssuedAt:  jwtgo.NewNumericDate(validIssuedAt),
					ExpiresAt: jwtgo.NewNumericDate(validExpiresAt),
					Audience:  jwtgo.ClaimStrings{validAudience},
				},
			},
			audience:   validAudience,
			uri:        validURI,
			body:       validBody,
			maxTimeout: 15 * time.Second,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.claims.Validate(tc.audience, tc.uri, tc.body, tc.maxTimeout)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
