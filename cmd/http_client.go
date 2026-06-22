package cmd

import (
	"net/http"
	"time"
)

// keepAlivesDisabledHTTPClient returns an HTTP client whose transport is a clone of
// http.DefaultTransport (preserving ProxyFromEnvironment and the other defaults) with
// keep-alives disabled. A fresh connection per request sidesteps stale-connection EOFs
// behind intermediaries that don't support HTTP connection reuse (e.g. kubectl
// port-forward), which is negligible at the one-shot protocol commands' RPC volume.
//
// Cloning rather than constructing a bare &http.Transport{DisableKeepAlives: true} keeps
// HTTP_PROXY/HTTPS_PROXY support intact for operators who reach the RPC endpoint via a proxy.
func keepAlivesDisabledHTTPClient(timeout time.Duration) *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DisableKeepAlives = true
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
	}
}
