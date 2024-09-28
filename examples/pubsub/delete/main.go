package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func deleteEventsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-delete"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsClient.Delete(ctx, "events-channel"); err != nil {
		log.Fatal(err)
	}
}

func deleteEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-delete"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsStoreClient.Delete(ctx, "events-store-channel"); err != nil {
		log.Fatal(err)
	}
}

func main() {
	deleteEventsChannel()
	deleteEventsStoreChannel()
}
