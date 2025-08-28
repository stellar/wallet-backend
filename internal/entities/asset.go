package entities

type Asset struct {
	Code   string `json:"code"   validate:"required,asset_code"`
	Issuer string `json:"issuer" validate:"required,asset_issuer"`
}
