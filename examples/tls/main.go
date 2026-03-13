// Package main demonstrates TLS, mTLS, and auth token connections with the KubeMQ Go SDK v2.
//
// This example shows how to connect with:
// - TLS certificate file (server-side TLS)
// - mTLS (mutual TLS with client cert + key + CA)
// - Authentication token
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

	// -------------------------------------------------------------------------
	// Example 1: Connect with server-side TLS (CA cert) + auth token
	// -------------------------------------------------------------------------
	tlsClient, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithTLS("path/to/ca-cert.pem"),
		kubemq.WithAuthToken("your-jwt-token"),
	)
	if err != nil {
		log.Fatal("TLS connect:", err)
	}
	defer tlsClient.Close()

	info, err := tlsClient.Ping(ctx)
	if err != nil {
		log.Fatal("Ping failed:", err)
	}
	fmt.Printf("TLS: connected to %s (version %s)\n", info.Host, info.Version)

	// -------------------------------------------------------------------------
	// Example 2: Connect with mutual TLS (client cert + key + CA)
	// -------------------------------------------------------------------------
	mtlsClient, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithMTLS(
			"path/to/client-cert.pem",
			"path/to/client-key.pem",
			"path/to/ca-cert.pem",
		),
	)
	if err != nil {
		log.Fatal("mTLS connect:", err)
	}
	defer mtlsClient.Close()

	info2, err := mtlsClient.Ping(ctx)
	if err != nil {
		log.Fatal("mTLS Ping failed:", err)
	}
	fmt.Printf("mTLS: connected to %s (version %s)\n", info2.Host, info2.Version)
}
