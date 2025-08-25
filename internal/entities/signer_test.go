package entities

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
)

func TestValidateSignersWeights(t *testing.T) {
	kp1 := keypair.MustRandom()
	kp2 := keypair.MustRandom()

	type testCase struct {
		name            string
		signers         []Signer
		wantErrContains string
		wantResult      int
	}

	testCases := []testCase{
		{
			name:            "🟢no_signers",
			signers:         []Signer{},
			wantErrContains: "",
			wantResult:      1,
		},
		{
			name: "🔴no_full_signers",
			signers: []Signer{
				{
					Address: kp1.Address(),
					Weight:  10,
					Type:    PartialSignerType,
				},
			},
			wantErrContains: "no full signers provided",
			wantResult:      0,
		},
		{
			name: "🔴full_signers_with_not_same_weight",
			signers: []Signer{
				{
					Address: kp1.Address(),
					Weight:  20,
					Type:    FullSignerType,
				},
				{
					Address: kp2.Address(),
					Weight:  19,
					Type:    FullSignerType,
				},
			},
			wantErrContains: "all full signers must have the same weight",
			wantResult:      0,
		},
		{
			name: "🔴partial_signer_should_be_less_than_full_signers",
			signers: []Signer{
				{
					Address: kp1.Address(),
					Weight:  20,
					Type:    FullSignerType,
				},
				{
					Address: kp2.Address(),
					Weight:  20,
					Type:    PartialSignerType,
				},
			},
			wantErrContains: "all partial signers' weights must be less than the weight of full signers",
			wantResult:      0,
		},
		{
			name: "🟢full_signers_same_weight",
			signers: []Signer{
				{
					Address: kp1.Address(),
					Weight:  20,
					Type:    FullSignerType,
				},
				{
					Address: kp2.Address(),
					Weight:  20,
					Type:    FullSignerType,
				},
			},
			wantErrContains: "",
			wantResult:      20,
		},
		{
			name: "🟢full_signers_and_partial_signers",
			signers: []Signer{
				{
					Address: kp1.Address(),
					Weight:  20,
					Type:    FullSignerType,
				},
				{
					Address: kp2.Address(),
					Weight:  10,
					Type:    PartialSignerType,
				},
			},
			wantErrContains: "",
			wantResult:      20,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			fw, err := ValidateSignersWeights(tc.signers)

			if tc.wantErrContains != "" {
				assert.EqualError(t, err, tc.wantErrContains)
				assert.Empty(t, fw)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.wantResult, fw)
			}
		})
	}
}
