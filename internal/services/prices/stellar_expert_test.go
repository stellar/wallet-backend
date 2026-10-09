package prices

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStellarExpertClient_AssetPriceUSD(t *testing.T) {
	tests := []struct {
		name     string
		token    string
		status   int
		body     string
		wantPath string
		want     *float64
		wantErr  bool
	}{
		{"price", "CTOK", 200, `{"price":0.25}`, "/asset/CTOK", ptr(0.25), false},
		{"no price", "CTOK", 200, `{"asset":"x"}`, "/asset/CTOK", nil, false},
		{"zero price", "CTOK", 200, `{"price":0}`, "/asset/CTOK", nil, false},
		{"not indexed", "CTOK", 404, ``, "/asset/CTOK", nil, false},
		{"rate limited", "CTOK", 429, ``, "/asset/CTOK", nil, true},
		{"server error", "CTOK", 500, ``, "/asset/CTOK", nil, true},
		{"malformed", "CTOK", 200, `{not json`, "/asset/CTOK", nil, true},
		{"native id", "XLM", 200, `{"price":0.1}`, "/asset/XLM", ptr(0.1), false},
		{"classic id", "USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", 200, `{"price":1}`, "/asset/USDC-GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN", ptr(1.0), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotPath, gotUA string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotPath, gotUA = r.URL.Path, r.Header.Get("User-Agent")
				w.WriteHeader(tc.status)
				if _, err := w.Write([]byte(tc.body)); err != nil {
					t.Error(err)
				}
			}))
			defer srv.Close()

			c := NewStellarExpertClient(srv.URL+"/", &http.Client{Timeout: 10 * time.Second})
			got, err := c.AssetPriceUSD(context.Background(), tc.token)
			assert.Equal(t, tc.wantPath, gotPath)
			assert.Equal(t, "wallet-backend", gotUA)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func ptr(f float64) *float64 { return &f }
