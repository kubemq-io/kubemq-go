// Example: connection/ping
//
// Demonstrates how to ping a KubeMQ server to verify connectivity and
// retrieve server information (host, version, uptime).
//
// Channel: go-connection.ping
// Client ID: go-connection-ping-client
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
		kubemq.WithClientId("go-connection-ping-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Ping the server to verify connectivity and retrieve server info.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Ping OK: host=%s version=%s uptime=%ds\n",
		info.Host, info.Version, info.ServerUpTimeSeconds)
}
