package entities

import "fmt"

type SignerType string

func (st SignerType) IsValid() bool {
	switch st {
	case FullSignerType, PartialSignerType:
		return true
	default:
		return false
	}
}

const (
	FullSignerType    SignerType = "full"
	PartialSignerType SignerType = "partial"
)

type Signer struct {
	Address string     `json:"address" validate:"required,public_key"`
	Weight  int        `json:"weight" validate:"gte=1"`
	Type    SignerType `json:"type" validate:"required,oneof=full partial"`
}

// ValidateSignersWeights will validate:
//  1. all full signers have the same weight
//  2. all partial signers weights are less than the full signer weight
//  3. there's at least one full signer.
func ValidateSignersWeights(signers []Signer) (int, error) {
	// firstly make sure all full signers have the same weight
	var fullSignerWeight *int
	for _, s := range signers {
		if s.Type == FullSignerType {
			if fullSignerWeight == nil {
				bufferWeight := s.Weight
				fullSignerWeight = &bufferWeight
			} else if s.Weight != *fullSignerWeight {
				return 0, fmt.Errorf("all full signers must have the same weight")
			}
		}
	}

	// secondly confirm there is at least one full signer
	if fullSignerWeight == nil {
		return 0, fmt.Errorf("no full signers provided")
	}

	// finally make sure all partial signers weights are lower than the full signers weight
	for _, s := range signers {
		if s.Type == PartialSignerType && s.Weight >= *fullSignerWeight {
			return 0, fmt.Errorf("all partial signers weight must be less than the weight of full signers")
		}
	}

	return *fullSignerWeight, nil
}
