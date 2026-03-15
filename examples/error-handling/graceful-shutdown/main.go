// Example: error-handling/graceful-shutdown
//
// Demonstrates graceful shutdown with OS signal handling.
// The client properly drains in-flight operations and closes
// subscriptions when receiving SIGINT or SIGTERM.
//
// Channel: go-error-handling.graceful-shutdown
// Client ID: go-error-handling-graceful-shutdown-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	// Create a context that is cancelled on OS signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-error-handling-graceful-shutdown-client"),
		kubemq.WithDrainTimeout(10*time.Second),
		kubemq.WithOnClosed(func() {
			fmt.Println("[Shutdown] Connection closed")
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	channel := "go-error-handling.graceful-shutdown"

	// Start a subscription that runs until shutdown.
	sub, err := client.SubscribeToEvents(ctx, channel, "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("Received: body=%s\n", event.Body)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Running... Press Ctrl+C to initiate graceful shutdown")

	// Wait for the shutdown signal.
	<-ctx.Done()
	fmt.Println("\nShutdown signal received, cleaning up...")

	// Cancel subscription first.
	sub.Unsubscribe()
	fmt.Println("Subscription cancelled")

	// Close the client, draining in-flight operations.
	if err := client.Close(); err != nil {
		log.Printf("Close error: %v", err)
	}
	fmt.Println("Graceful shutdown complete")
}
