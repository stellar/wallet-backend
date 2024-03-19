package main

import (
    "context"
    _ "embed"
    "os"

    "github.com/tetratelabs/wazero"
    "github.com/tetratelabs/wazero/experimental/sock"
    "github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
)

const port uint32 = 3000
const socketDescriptor uint32 = 3
const address string = "127.0.0.1"

//go:embed wasm/module.wasm
var moduleData []byte

func main() {
    // The availability of networking is passed to the guest module via context.
    // AFAIK there is not yet any bespoke API to figure out
    // a) dynamic port allocation,
    // b) the file descriptor.
    // However, we can make an educated guess of the latter: since stdin,
    // stdout and stderr are the file descriptors 0-2, our socket SHOULD be 3.
    // Take note that this guess is for the perspective of the guest module.
    ctx := sock.WithConfig(
        context.Background(),
        sock.NewConfig().WithTCPListener(address, int(port)),
    )

    // Runtime and WASI prep.
    r := wazero.NewRuntime(ctx)
    defer r.Close(ctx)
    wasi_snapshot_preview1.MustInstantiate(ctx, r)

    // Module configuration.
    cfg := wazero.NewModuleConfig().WithStdout(os.Stdout).WithStderr(os.Stderr)
    // stdout/stderr added for simple debugging: this breaks sandboxing.

    // Export a function for the guest to fetch the (guessed) fd.
    _, err := r.NewHostModuleBuilder("env").NewFunctionBuilder().
        WithFunc(func() uint32 {
            return port
        }).Export("getPort").
        NewFunctionBuilder().
        WithFunc(func() uint32 {
            return socketDescriptor
        }).Export("getSocketDescriptor").
        Instantiate(ctx)
    
    if err != nil {
        panic(err)
    }
    // We also could provide the fd via an environment variable,
    // but working with strings can be annoying:
    // cfg = cfg.WithEnv("socketDescriptor", fmt.Sprint(socketDescriptor))

    // Compilation step
    compiled, err := r.CompileModule(ctx, moduleData)
    if err != nil {
        panic(err)
    }

    // Run the module
    if _, err := r.InstantiateModule(ctx, compiled, cfg); err != nil {
        panic(err)
    }
}
