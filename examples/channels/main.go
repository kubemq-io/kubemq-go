// Package main demonstrates Channel management with the KubeMQ Go SDK v2.
//
// This example creates, lists, and deletes a channel. Run with a
// KubeMQ server on localhost:50000 (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
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
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channelName := "example-channel"

	// Create a channel
	err = client.CreateChannel(ctx, channelName, kubemq.ChannelTypeEvents)
	if err != nil {
		log.Fatal("CreateChannel:", err)
	}
	fmt.Printf("Channel %q created\n", channelName)

	// List channels
	channels, err := client.ListChannels(ctx, kubemq.ChannelTypeEvents, "")
	if err != nil {
		log.Fatal("ListChannels:", err)
	}
	fmt.Printf("Found %d channels:\n", len(channels))
	for _, ch := range channels {
		fmt.Printf("  - %s (active=%v)\n", ch.Name, ch.IsActive)
	}

	// Delete the channel
	err = client.DeleteChannel(ctx, channelName, kubemq.ChannelTypeEvents)
	if err != nil {
		log.Fatal("DeleteChannel:", err)
	}
	fmt.Printf("Channel %q deleted\n", channelName)
}
