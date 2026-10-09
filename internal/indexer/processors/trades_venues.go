package processors

import (
	"fmt"

	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// TradeVenues is the per-network set of fixed addresses the trades processor trusts. An empty
// address disables that venue's decoder. Anchor tokens are the counter side of every priced fill:
// USDC ranks before XLM, and everything else ranks after both.
type TradeVenues struct {
	XLMSAC          string
	USDCSAC         string
	SoroswapFactory string
	AquariusRouter  string
}

// usdcIssuers maps a network passphrase to the Circle USDC issuer on that network.
var usdcIssuers = map[string]string{
	network.PublicNetworkPassphrase: "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN",
	network.TestNetworkPassphrase:   "GBBD47IF6LWK7P7MDEVSCWR7DPUWV3NY3DTQEVFL4NAT4AQH3ZLLFLA5",
}

// soroswapFactories and aquariusRouters are the venues' fixed deployment addresses. Only pubnet
// is verified; a network without an entry runs with that venue disabled.
var (
	soroswapFactories = map[string]string{
		network.PublicNetworkPassphrase: "CA4HEQTL2WPEUYKYKCDOHCDNIV4QHNJ7EL4J4NQ6VADP7SYHVRYZ7AW2",
	}
	aquariusRouters = map[string]string{
		network.PublicNetworkPassphrase: "CBQDHNBFBZYE4MKPWBSJOPIYLW4SFSXAXUTSXJN76GNKYVYPCKWC6QUK",
	}
)

// TradeVenuesFor resolves the venue addresses for a network. The anchor SACs are derived from the
// passphrase, so they are always present.
func TradeVenuesFor(networkPassphrase string) (TradeVenues, error) {
	xlm, err := SACAddress(xdr.MustNewNativeAsset(), networkPassphrase)
	if err != nil {
		return TradeVenues{}, fmt.Errorf("deriving XLM SAC: %w", err)
	}
	v := TradeVenues{
		XLMSAC:          xlm,
		SoroswapFactory: soroswapFactories[networkPassphrase],
		AquariusRouter:  aquariusRouters[networkPassphrase],
	}
	if issuer, ok := usdcIssuers[networkPassphrase]; ok {
		usdc, err := xdr.NewCreditAsset("USDC", issuer)
		if err != nil {
			return TradeVenues{}, fmt.Errorf("building USDC asset: %w", err)
		}
		if v.USDCSAC, err = SACAddress(usdc, networkPassphrase); err != nil {
			return TradeVenues{}, fmt.Errorf("deriving USDC SAC: %w", err)
		}
	}
	return v, nil
}

// SACAddress returns the Stellar Asset Contract C-address of a classic asset on a network.
func SACAddress(asset xdr.Asset, networkPassphrase string) (string, error) {
	id, err := asset.ContractID(networkPassphrase)
	if err != nil {
		return "", fmt.Errorf("computing contract id for %s: %w", asset.StringCanonical(), err)
	}
	addr, err := strkey.Encode(strkey.VersionByteContract, id[:])
	if err != nil {
		return "", fmt.Errorf("encoding contract id for %s: %w", asset.StringCanonical(), err)
	}
	return addr, nil
}
