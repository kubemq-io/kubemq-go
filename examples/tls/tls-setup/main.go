// Example: tls/tls-setup
//
// Demonstrates how to connect to a KubeMQ server using server-side TLS.
// The client verifies the server's certificate using a CA certificate file.
//
// Channel: go-tls.tls-setup
// Client ID: go-tls-tls-setup-client
//
// Run with a KubeMQ server configured with TLS.
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

	// Connect with server-side TLS using a CA certificate file.
	// Replace "path/to/ca-cert.pem" with the actual path to your CA certificate.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithClientId("go-tls-tls-setup-client"),
		kubemq.WithTLS("path/to/ca-cert.pem"),
	)
	if err != nil {
		log.Fatalf("TLS connection failed: %v", err)
	}
	defer client.Close()

	// Verify the secure connection.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("TLS connected: host=%s version=%s\n", info.Host, info.Version)
}
