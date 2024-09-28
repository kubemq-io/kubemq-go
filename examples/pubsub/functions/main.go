package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendSubscribeEvents() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-send-subscribe"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subReq := &kubemq.EventsSubscription{
		Channel:  "events-channel",
		Group:    "",
		ClientId: "",
	}
	err = eventsClient.Subscribe(ctx, subReq, func(msg *kubemq.Event, err error) {
		log.Println(msg.String())
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(300 * time.Second)

	err = eventsClient.Send(ctx, &kubemq.Event{
		Channel:  "events-channel",
		Metadata: "some-metadata",
		Body:     []byte("hello kubemq - sending event"),
	})

	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
}

func sendSubscribeEventsStore() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-send-subscribe"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subReq := &kubemq.EventsStoreSubscription{
		Channel:          "events-store-channel",
		Group:            "",
		ClientId:         "",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}
	err = eventsStoreClient.Subscribe(ctx, subReq, func(msg *kubemq.EventStoreReceive, err error) {
		log.Println(msg.String())
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	result, err := eventsStoreClient.Send(ctx, &kubemq.EventStore{
		Channel:  "events-store-channel",
		Metadata: "some-metadata",
		Body:     []byte("hello kubemq - sending event store"),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	time.Sleep(1 * time.Second)

}
func main() {
	sendSubscribeEvents()
	sendSubscribeEventsStore()
}
