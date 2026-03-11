// Package main demonstrates TLS and auth token connections with the KubeMQ Go SDK v2.
//
// This example shows how to connect with TLS certificates and authentication tokens.
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

	// Example: connect with TLS certificate file
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq.example.com", 50000),
		kubemq.WithCredentials("path/to/cert.pem", ""),
		kubemq.WithAuthToken("your-jwt-token"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatal("Ping failed:", err)
	}
	fmt.Printf("Connected to %s (version %s) via TLS\n", info.Host, info.Version)
}
