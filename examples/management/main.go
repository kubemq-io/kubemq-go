// Package main demonstrates Channel Management features with the KubeMQ Go SDK v2.
//
// This example covers: queue info (all/specific), list channels with search filter,
// create/delete channels, and purge queue.
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
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// -------------------------------------------------------------------------
	// 1. Create channels of various types
	// -------------------------------------------------------------------------
	err = client.CreateChannel(ctx, "mgmt-events-ch", kubemq.ChannelTypeEvents)
	if err != nil {
		log.Println("CreateChannel events:", err)
	} else {
		fmt.Println("Created events channel: mgmt-events-ch")
	}

	err = client.CreateChannel(ctx, "mgmt-queue-ch", kubemq.ChannelTypeQueues)
	if err != nil {
		log.Println("CreateChannel queues:", err)
	} else {
		fmt.Println("Created queue channel: mgmt-queue-ch")
	}

	// -------------------------------------------------------------------------
	// 2. List all channels of a type
	// -------------------------------------------------------------------------
	channels, err := client.ListChannels(ctx, kubemq.ChannelTypeEvents, "")
	if err != nil {
		log.Println("ListChannels:", err)
	} else {
		fmt.Printf("Events channels: %d found\n", len(channels))
		for _, ch := range channels {
			fmt.Printf("  - %s (active=%v)\n", ch.Name, ch.IsActive)
		}
	}

	// -------------------------------------------------------------------------
	// 3. List channels with search filter
	// -------------------------------------------------------------------------
	filtered, err := client.ListChannels(ctx, kubemq.ChannelTypeEvents, "mgmt-")
	if err != nil {
		log.Println("ListChannels filtered:", err)
	} else {
		fmt.Printf("Filtered events channels (prefix 'mgmt-'): %d found\n", len(filtered))
		for _, ch := range filtered {
			fmt.Printf("  - %s\n", ch.Name)
		}
	}

	// -------------------------------------------------------------------------
	// 4. Queue info — all queues
	// -------------------------------------------------------------------------
	info, err := client.QueuesInfo(ctx, "")
	if err != nil {
		log.Println("QueuesInfo all:", err)
	} else {
		fmt.Printf("Queue info: total=%d sent=%d delivered=%d waiting=%d\n",
			info.TotalQueue, info.Sent, info.Delivered, info.Waiting)
		for _, q := range info.Queues {
			fmt.Printf("  queue=%s messages=%d bytes=%d firstSeq=%d lastSeq=%d subscribers=%d\n",
				q.Name, q.Messages, q.Bytes, q.FirstSeq, q.LastSeq, q.Subscribers)
		}
	}

	// -------------------------------------------------------------------------
	// 5. Queue info — specific queue
	// -------------------------------------------------------------------------
	specificInfo, err := client.QueuesInfo(ctx, "mgmt-queue-ch")
	if err != nil {
		log.Println("QueuesInfo specific:", err)
	} else {
		fmt.Printf("Specific queue info: total=%d\n", specificInfo.TotalQueue)
		for _, q := range specificInfo.Queues {
			fmt.Printf("  queue=%s messages=%d waiting=%d\n", q.Name, q.Messages, q.Waiting)
		}
	}

	// -------------------------------------------------------------------------
	// 6. Purge queue (via AckAllQueueMessages)
	// -------------------------------------------------------------------------
	// First send a message to the queue so there's something to purge
	msg := kubemq.NewQueueMessage().
		SetChannel("mgmt-queue-ch").
		SetBody([]byte("to-be-purged"))
	_, _ = client.SendQueueMessage(ctx, msg)

	purgeResp, err := client.AckAllQueueMessages(ctx, &kubemq.AckAllQueueMessagesRequest{
		Channel:         "mgmt-queue-ch",
		WaitTimeSeconds: 5,
	})
	if err != nil {
		log.Println("Purge queue:", err)
	} else {
		fmt.Printf("Purge queue: acknowledged %d messages\n", purgeResp.AffectedMessages)
	}

	// -------------------------------------------------------------------------
	// 7. Delete channels
	// -------------------------------------------------------------------------
	err = client.DeleteChannel(ctx, "mgmt-events-ch", kubemq.ChannelTypeEvents)
	if err != nil {
		log.Println("DeleteChannel events:", err)
	} else {
		fmt.Println("Deleted events channel: mgmt-events-ch")
	}

	err = client.DeleteChannel(ctx, "mgmt-queue-ch", kubemq.ChannelTypeQueues)
	if err != nil {
		log.Println("DeleteChannel queues:", err)
	} else {
		fmt.Println("Deleted queue channel: mgmt-queue-ch")
	}
}
