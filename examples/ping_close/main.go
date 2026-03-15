// Package main demonstrates Ping and Close lifecycle operations with the KubeMQ Go SDK v2.
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
	)
	if err != nil {
		log.Fatal(err)
	}

	// Ping the server to verify connectivity and retrieve server info.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Ping OK: host=%s version=%s uptime=%ds\n",
		info.Host, info.Version, info.ServerUpTimeSeconds)

	// Close the client gracefully, draining in-flight operations.
	if err := client.Close(); err != nil {
		log.Fatalf("Close failed: %v", err)
	}
	fmt.Println("Client closed successfully")
}
