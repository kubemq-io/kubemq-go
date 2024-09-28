package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listEventsChannels() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-lister"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := eventsClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}

func listEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-lister"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := eventsStoreClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}

func main() {
	listEventsChannels()
	listEventsStoreChannel()
}
