package contracts

import (
	"fmt"

	"github.com/stellar/go/xdr"
)

// extractAddressFromScVal helps extract a string representation of an address from a ScVal
func extractAddressFromScVal(val xdr.ScVal) (string, error) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", fmt.Errorf("invalid address")
	}
	addrStr, err := addr.String()
	if err != nil {
		return "", fmt.Errorf("failed to convert address to string: %w", err)
	}
	return addrStr, nil
}

// extractAssetFromScVal helps extract an asset from a ScVal
func extractAssetFromScVal(val xdr.ScVal) (xdr.Asset, error) {
	asset, ok := val.GetStr()
	if !ok {
		return xdr.Asset{}, fmt.Errorf("invalid asset")
	}
	assets, err := xdr.BuildAssets(string(asset))
	if err != nil {
		return xdr.Asset{}, fmt.Errorf("failed to build assets: %w", err)
	}
	if len(assets) == 0 {
		return xdr.Asset{}, fmt.Errorf("no assets found")
	}
	return assets[0], nil
}
