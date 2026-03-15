// Example: connection/token-auth
//
// Demonstrates how to connect to a KubeMQ server with authentication
// using a JWT token. The WithAuthToken option sets a static token that
// is sent with every request.
//
// Channel: go-connection.token-auth
// Client ID: go-connection-token-auth-client
//
// Run with a KubeMQ server configured for token authentication.
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

	// Connect with a static JWT authentication token.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-connection-token-auth-client"),
		kubemq.WithAuthToken("your-jwt-token-here"),
	)
	if err != nil {
		log.Fatalf("Failed to connect with token auth: %v", err)
	}
	defer client.Close()

	// Verify the authenticated connection.
	info, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Printf("Authenticated connection: host=%s version=%s\n",
		info.Host, info.Version)
}
