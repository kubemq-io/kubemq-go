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
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-event-store-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer sender.Close()

	receiver, err := kubemq.NewClient(ctx,
		kubemq.WithUri("http://localhost:9090"),
		kubemq.WithClientId("test-event-rest-client"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)
	}
	defer receiver.Close()
	channelName := "testing_event_store-channelName"

	//sending 10 single events to store
	for i := 0; i < 10; i++ {
		result, err := sender.ES().
			SetId(fmt.Sprintf("event-store-%d", i)).
			SetChannel(channelName).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending single event to store")).
			AddTag("seq", fmt.Sprintf("%d", i)).
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
	go sender.StreamEventsStore(ctx, eventsStoreStreamCh, eventsStoreSResultCh, errStreamCh)
	for i := 10; i < 20; i++ {
		event := sender.ES().
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
	eventsCh, err := receiver.SubscribeToEventsStore(ctx, channelName, "", errCh, kubemq.StartFromFirstEvent())
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		event, more := <-eventsCh
		if !more {
			log.Println("Next EventStore done")
			return
		}
		log.Printf("Next EventStore\nSequence: %d\nTime: %s\nBody: %s\n", event.Sequence, event.Timestamp, event.Body)
	}

}
