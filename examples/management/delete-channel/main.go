// Example: management/delete-channel
//
// Demonstrates deleting a channel. First creates a channel, then deletes it.
//
// Channel: go-management.delete-channel
// Client ID: go-management-delete-channel-client
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
		kubemq.WithClientId("go-management-delete-channel-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channelName := "go-management.delete-channel.temp"

	// Create a channel to demonstrate deletion.
	err = client.CreateChannel(ctx, channelName, kubemq.ChannelTypeEvents)
	if err != nil {
		log.Printf("CreateChannel: %v", err)
	} else {
		fmt.Printf("Created channel: %s\n", channelName)
	}

	// Delete the channel.
	err = client.DeleteChannel(ctx, channelName, kubemq.ChannelTypeEvents)
	if err != nil {
		log.Printf("DeleteChannel: %v", err)
	} else {
		fmt.Printf("Deleted channel: %s\n", channelName)
	}

	// Typed convenience method — no channel-type constant needed.
	typedName := "go-management.delete-channel.typed"
	_ = client.CreateEventsChannel(ctx, typedName)
	err = client.DeleteEventsChannel(ctx, typedName)
	if err != nil {
		log.Printf("DeleteEventsChannel: %v", err)
	} else {
		fmt.Printf("Deleted events channel (typed): %s\n", typedName)
	}
}
