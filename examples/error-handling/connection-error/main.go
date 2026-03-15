// Example: error-handling/connection-error
//
// Demonstrates handling connection errors when the KubeMQ server is
// unreachable. Shows how to use WithCheckConnection to fail fast on
// startup, and how to handle connection failures gracefully.
//
// Channel: go-error-handling.connection-error
// Client ID: go-error-handling-connection-error-client
//
// This example intentionally connects to a non-existent server.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Attempt to connect to a non-existent server with CheckConnection enabled.
	// This causes NewClient to fail fast if the server is unreachable.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 59999), // Non-existent server
		kubemq.WithClientId("go-error-handling-connection-error-client"),
		kubemq.WithCheckConnection(true),
		kubemq.WithConnectionTimeout(3*time.Second),
	)
	if err != nil {
		fmt.Printf("Connection failed (expected): %v\n", err)
		fmt.Println("Tip: Use WithCheckConnection(true) to detect unreachable servers at startup")
		return
	}
	defer client.Close()

	// If we get here, verify with a ping.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Printf("Ping failed: %v", err)
		return
	}
	fmt.Printf("Connected: %s\n", info.Host)
}
