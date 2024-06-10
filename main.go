package main

import (
	"github.com/stellar/wallet-backend/cmd"
)

// GitCommit is populated at build time by
// go build -ldflags "-X main.GitCommit=$GIT_COMMIT"
var GitCommit string

func main() {
	cmd.Execute()
}
