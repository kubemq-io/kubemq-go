// Example: connection/close
//
// Demonstrates how to gracefully close a KubeMQ client connection.
// Close drains in-flight operations before shutting down.
//
// Channel: go-connection.close
// Client ID: go-connection-close-client
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

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-connection-close-client"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Verify connectivity before closing.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Connected: host=%s version=%s\n", info.Host, info.Version)

	// Close the client gracefully, draining in-flight operations.
	if err := client.Close(); err != nil {
		log.Fatalf("Close failed: %v", err)
	}
	fmt.Println("Client closed successfully")
}
