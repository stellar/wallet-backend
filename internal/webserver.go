package main

import (
	"fmt"
	"net/http"
)

func main() {
	// Define a handler function
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Request received")
		w.WriteHeader(http.StatusOK) // Send HTTP 200 status code
		w.Write([]byte("OK"))        // Optional: send a response body
	})

	// Start the server on localhost:1234
	fmt.Println("Starting server on http://localhost:1234")
	err := http.ListenAndServe("localhost:1234", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
