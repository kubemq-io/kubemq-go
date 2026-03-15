// Example: connection/custom-timeouts
//
// Demonstrates how to configure custom timeouts for the KubeMQ client
// including connection timeout, keepalive, and drain timeout.
//
// Channel: go-connection.custom-timeouts
// Client ID: go-connection-custom-timeouts-client
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

	// Create a client with custom timeout configuration.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-connection-custom-timeouts-client"),
		// Custom connection timeout (default: 10s)
		kubemq.WithConnectionTimeout(15*time.Second),
		// Custom keepalive settings (default: 10s interval, 5s timeout)
		kubemq.WithKeepaliveTime(30*time.Second),
		kubemq.WithKeepaliveTimeout(10*time.Second),
		// Custom drain timeout for Close() (default: 5s)
		kubemq.WithDrainTimeout(10*time.Second),
		// Max message sizes
		kubemq.WithMaxReceiveMessageSize(50*1024*1024), // 50 MB
		kubemq.WithMaxSendMessageSize(50*1024*1024),    // 50 MB
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Connected with custom timeouts: host=%s version=%s\n",
		info.Host, info.Version)
}
