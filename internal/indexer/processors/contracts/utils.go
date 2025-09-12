package contracts

import (
	"fmt"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

// extractAddress helps extract a string representation of an address from a ScVal
func extractAddress(val xdr.ScVal) (string, error) {
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

// extractAsset helps extract an asset from a ScVal
func extractAsset(val xdr.ScVal) (xdr.Asset, error) {
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

func isSAC(expectedContractID string, asset xdr.Asset, networkPassphrase string) bool {
	contractID, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return false
	}
	return strkey.MustEncode(strkey.VersionByteContract, contractID[:]) == expectedContractID
}
