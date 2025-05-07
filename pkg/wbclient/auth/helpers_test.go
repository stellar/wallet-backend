package auth

import (
	"errors"
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
)

func Test_ParseExpirationDuration(t *testing.T) {
	testCases := []struct {
		name             string
		err              error
		expectedDuration time.Duration
		expectedOk       bool
	}{
		{
			name:             "🔴no_error",
			err:              nil,
			expectedDuration: 0,
			expectedOk:       false,
		},
		{
			name:             "🔴non_validation_error",
			err:              errors.New("some other error"),
			expectedDuration: 0,
			expectedOk:       false,
		},
		{
			name: "🔴validation_error_without_expiration",
			err: &jwtgo.ValidationError{
				Inner:  errors.New("some validation error"),
				Errors: jwtgo.ValidationErrorSignatureInvalid,
			},
			expectedDuration: 0,
			expectedOk:       false,
		},
		{
			name: "🟢validation_error_with_expiration",
			err: &jwtgo.ValidationError{
				Inner:  errors.New("token is expired by 107.763ms"),
				Errors: jwtgo.ValidationErrorExpired,
			},
			expectedDuration: 107*time.Millisecond + 763*time.Microsecond,
			expectedOk:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			duration, ok := ParseExpirationDuration(tc.err)
			assert.Equal(t, tc.expectedDuration, duration)
			assert.Equal(t, tc.expectedOk, ok)
		})
	}
}
