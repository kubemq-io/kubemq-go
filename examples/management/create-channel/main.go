// Example: management/create-channel
//
// Demonstrates creating channels of different types (events, queues, etc.).
// Channels can be pre-created for organizational purposes.
//
// Channel: go-management.create-channel
// Client ID: go-management-create-channel-client
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
		kubemq.WithClientId("go-management-create-channel-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Create an events channel.
	err = client.CreateChannel(ctx, "go-management.create-channel.events", kubemq.ChannelTypeEvents)
	if err != nil {
		log.Println("CreateChannel events:", err)
	} else {
		fmt.Println("Created events channel: go-management.create-channel.events")
	}

	// Create a queues channel.
	err = client.CreateChannel(ctx, "go-management.create-channel.queues", kubemq.ChannelTypeQueues)
	if err != nil {
		log.Println("CreateChannel queues:", err)
	} else {
		fmt.Println("Created queue channel: go-management.create-channel.queues")
	}

	// Create an events store channel.
	err = client.CreateChannel(ctx, "go-management.create-channel.es", kubemq.ChannelTypeEventsStore)
	if err != nil {
		log.Println("CreateChannel events_store:", err)
	} else {
		fmt.Println("Created events store channel: go-management.create-channel.es")
	}
}
