package main

import (
    "fmt"
    "io"
    "net"
    "net/http"
    "os"
    "syscall"
)

//go:wasmimport env getPort
func getPort() uint32

//go:wasmimport env getSocketDescriptor
func getSocketDescriptor() uint32

func getRoot(w http.ResponseWriter, r *http.Request) {
    fmt.Printf("got request to /\n")
    io.WriteString(w, "THIS IS FROM WASM\n")
}

func getHello(w http.ResponseWriter, r *http.Request) {
    fmt.Printf("got request to /hello\n")
    io.WriteString(w, "HELLO FROM WASM\n")
}

func main() {
    // Receive the file descriptor of the open TCP socket from host.
    port := getPort()
    sd := getSocketDescriptor()

    // Blocking I/O is problematic due to the lack of threads.
    if err := syscall.SetNonblock(int(sd), true); err != nil {
        panic(err)
    }

    // The host SHOULD close the file descriptor when the context is done.
    // The file name is arbitrary.
    ln, err := net.FileListener(os.NewFile(uintptr(sd), "[socket]"))
    if err != nil {
        panic(err)
    }

    mux := http.NewServeMux()

    mux.HandleFunc("/", getRoot)
    fmt.Printf("handler registered to / on port %d\n", port)

    mux.HandleFunc("/hello", getHello)
    fmt.Printf("handler registered to /hello on port %d\n", port)

    // HTTP server
    if err := http.Serve(ln, mux); err != nil {
        panic(err)
    }
}
