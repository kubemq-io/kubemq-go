// Example: tls/mtls-setup
//
// Demonstrates mutual TLS (mTLS) where both client and server authenticate
// each other using certificates. Requires a client certificate, client key,
// and CA certificate.
//
// Channel: go-tls.mtls-setup
// Client ID: go-tls-mtls-setup-client
//
// Run with a KubeMQ server configured for mTLS.
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

	// Connect with mutual TLS using client cert, client key, and CA cert.
	// Replace the paths with your actual certificate files.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithClientId("go-tls-mtls-setup-client"),
		kubemq.WithMTLS(
			"path/to/client-cert.pem",
			"path/to/client-key.pem",
			"path/to/ca-cert.pem",
		),
	)
	if err != nil {
		log.Fatalf("mTLS connection failed: %v", err)
	}
	defer client.Close()

	// Verify the mutual TLS connection.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("mTLS connected: host=%s version=%s\n", info.Host, info.Version)
}
