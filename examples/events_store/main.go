package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-event-store-client-id"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channelName := "testing_event_store-channelName"

	//sending 10 single events to store
	for i := 0; i < 10; i++ {
		result, err := client.ES().
			SetId(fmt.Sprintf("event-store-%d", i)).
			SetChannel(channelName).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending single event to store")).
			Send(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Sending event #%d: Result: %t", i, result.Sent)
	}
	// sending another 10 single events to store via streaming
	eventsStoreStreamCh := make(chan *kubemq.EventStore, 1)
	eventsStoreSResultCh := make(chan *kubemq.EventStoreResult, 1)
	errStreamCh := make(chan error, 1)
	go client.StreamEventsStore(ctx, eventsStoreStreamCh, eventsStoreSResultCh, errStreamCh)
	for i := 10; i < 20; i++ {
		event := client.ES().
			SetId(fmt.Sprintf("event-store-%d", i)).
			SetChannel(channelName).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending stream event to store"))
		eventsStoreStreamCh <- event
		select {
		case err := <-errStreamCh:
			log.Println(err)
			return
		case result := <-eventsStoreSResultCh:
			log.Printf("Sending event #%d: Result: %t", i, result.Sent)
		}
	}

	// some delay to retrieve event
	time.Sleep(time.Second)
	errCh := make(chan error)
	eventsCh, err := client.SubscribeToEventsStore(ctx, channelName, "", errCh, kubemq.StartFromFirstEvent())
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		event := <-eventsCh
		log.Printf("Receive EventStore\nSequence: %d\nTime: %s\nBody: %s\n", event.Sequence, event.Timestamp, event.Body)
	}

}
