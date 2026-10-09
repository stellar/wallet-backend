package prices

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// StellarExpertNativeID is the asset id stellar.expert uses for the native token. Classic assets
// are addressed as CODE-ISSUER; a contract address resolves only for tokens that exist as
// contracts, and carries no price for a classic asset's SAC.
const StellarExpertNativeID = "XLM"

// StellarExpertClient reads asset prices from the stellar.expert explorer API.
type StellarExpertClient struct {
	baseURL string
	http    *http.Client
}

// NewStellarExpertClient builds a client for a base URL such as
// https://api.stellar.expert/explorer/public.
func NewStellarExpertClient(baseURL string, httpClient *http.Client) *StellarExpertClient {
	return &StellarExpertClient{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

// AssetPriceUSD returns the USD price stellar.expert reports for an asset id (XLM, CODE-ISSUER or
// a contract address). It returns nil without an error when the asset is not indexed or has no
// positive price.
func (c *StellarExpertClient) AssetPriceUSD(ctx context.Context, id string) (*float64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/asset/"+id, nil)
	if err != nil {
		return nil, fmt.Errorf("building stellar.expert request: %w", err)
	}
	req.Header.Set("User-Agent", "wallet-backend")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("requesting stellar.expert asset %s: %w", id, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("stellar.expert asset %s: unexpected status %d", id, resp.StatusCode)
	}

	var body struct {
		Price float64 `json:"price"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decoding stellar.expert asset %s: %w", id, err)
	}
	if body.Price <= 0 {
		return nil, nil
	}
	return &body.Price, nil
}
