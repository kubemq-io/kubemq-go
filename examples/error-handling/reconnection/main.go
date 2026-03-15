// Example: error-handling/reconnection
//
// Demonstrates configuring automatic reconnection with state callbacks.
// The client registers callbacks for connection state transitions
// (connected, disconnected, reconnecting, reconnected, closed).
//
// Channel: go-error-handling.reconnection
// Client ID: go-error-handling-reconnection-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Configure reconnection policy and state callbacks.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-error-handling-reconnection-client"),
		// Custom reconnect policy with exponential backoff.
		kubemq.WithReconnectPolicy(kubemq.ReconnectPolicy{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
			MaxAttempts:  0, // 0 = unlimited
		}),
		// State callbacks to monitor connection lifecycle.
		kubemq.WithOnConnected(func() {
			fmt.Println("[State] Connected to KubeMQ server")
		}),
		kubemq.WithOnDisconnected(func() {
			fmt.Println("[State] Disconnected from KubeMQ server")
		}),
		kubemq.WithOnReconnecting(func() {
			fmt.Println("[State] Reconnecting to KubeMQ server...")
		}),
		kubemq.WithOnReconnected(func() {
			fmt.Println("[State] Reconnected to KubeMQ server")
		}),
		kubemq.WithOnClosed(func() {
			fmt.Println("[State] Connection closed")
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Check connection state.
	state := client.State()
	fmt.Printf("Current state: %v\n", state)

	// Verify connectivity.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Printf("Ping failed: %v", err)
		return
	}
	fmt.Printf("Connected: host=%s version=%s\n", info.Host, info.Version)
	fmt.Println("Client is configured with automatic reconnection")
}
