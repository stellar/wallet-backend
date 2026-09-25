package contracts

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar/wallet-backend/internal/indexer/types"
)

// extractAddressFromScVal extracts the strkey form of an address ScVal, rejecting any kind
// an account column cannot store so the caller skips the event. Muxed addresses reduce to
// their base account (see types.StorableAddressString).
func extractAddressFromScVal(val xdr.ScVal) (string, error) {
	addr, ok := val.GetAddress()
	if !ok {
		return "", fmt.Errorf("invalid address")
	}
	addrStr, storable, err := types.StorableAddressString(addr)
	if err != nil {
		return "", fmt.Errorf("failed to convert address to string: %w", err)
	}
	if !storable {
		return "", fmt.Errorf("unsupported address type %s", addr.Type)
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
