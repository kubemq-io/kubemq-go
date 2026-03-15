// Example: management/list-channels
//
// Demonstrates listing channels with optional search filter.
// You can list all channels of a type or filter by name prefix.
//
// Channel: go-management.list-channels
// Client ID: go-management-list-channels-client
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-management-list-channels-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// List all events channels.
	channels, err := client.ListChannels(ctx, kubemq.ChannelTypeEvents, "")
	if err != nil {
		log.Println("ListChannels:", err)
	} else {
		fmt.Printf("All events channels: %d found\n", len(channels))
		for _, ch := range channels {
			fmt.Printf("  - %s (active=%v)\n", ch.Name, ch.IsActive)
		}
	}

	// List channels with a search filter.
	filtered, err := client.ListChannels(ctx, kubemq.ChannelTypeQueues, "go-")
	if err != nil {
		log.Println("ListChannels filtered:", err)
	} else {
		fmt.Printf("Queue channels matching 'go-': %d found\n", len(filtered))
		for _, ch := range filtered {
			fmt.Printf("  - %s\n", ch.Name)
		}
	}
}
