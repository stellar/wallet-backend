# wallet-backend
A backend for Stellar wallet applications

## Setup
Build the module
- `GOOS="wasip1" GOARCH="wasm" go build -o wasm/module.wasm wasm/module.go`

## Running the service
`go run main.go`

## Testing the service
cURL the API endpoints
- `curl http://localhost:3000/`
- `curl http://localhost:3000/hello`
