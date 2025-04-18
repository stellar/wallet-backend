package main

import (
	"github.com/stellar/wallet-backend/cmd"
)

// Version is the official version of this application.
const Version = "0.2.0"

// GitCommit is populated at build time by
// go build -ldflags "-X main.GitCommit=$GIT_COMMIT"
var GitCommit string

func main() {
	cmd.Execute(cmd.RootConfig{
		GitCommit: GitCommit,
		Version:   Version,
	})
}
