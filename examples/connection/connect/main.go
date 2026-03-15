// Example: connection/connect
//
// Demonstrates how to create a basic KubeMQ client connection.
// The client connects to a KubeMQ server on localhost:50000 and verifies
// connectivity with a Ping.
//
// Channel: go-connection.connect
// Client ID: go-connection-connect-client
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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a KubeMQ client with basic configuration.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-connection-connect-client"),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	// Verify the connection by pinging the server.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Connected successfully: host=%s version=%s uptime=%ds\n",
		info.Host, info.Version, info.ServerUpTimeSeconds)
}
