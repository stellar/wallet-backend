package main

import (
	"github.com/stellar/wallet-backend/cmd"
)

// Version and GitCommit are set at build time:
//
//	go build -ldflags "-X main.Version=v1.2.3 -X main.GitCommit=abc1234"
//
// A build without ldflags reports "dev" and an empty commit.
var (
	Version   = "dev"
	GitCommit string
)

func main() {
	cmd.Execute(cmd.RootConfig{
		GitCommit: GitCommit,
		Version:   Version,
	})
}
