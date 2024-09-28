package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func createEventsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-creator"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsClient.Create(ctx, "events-channel"); err != nil {
		log.Fatal(err)
	}
}

func createEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-creator"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsStoreClient.Create(ctx, "events-store-channel"); err != nil {
		log.Fatal(err)
	}
}

func main() {
	createEventsChannel()
	createEventsStoreChannel()
}
