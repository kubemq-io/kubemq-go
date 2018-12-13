package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/go"
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
	channel := "testing_event_store-channel"

	//sending 10 events to store
	for i := 0; i < 10; i++ {
		result, err := client.ES().
			SetId(fmt.Sprintf("event-store-%d", i)).
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending single event to store")).
			Send(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Sending event #d: Result: %t", result.Sent)
	}
	// some delay to retrieve event
	time.Sleep(time.Second)
	errCh := make(chan error)
	eventsCh, err := client.SubscribeToEventsStore(ctx, channel, "", errCh, kubemq.StartFromFirstEvent())
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		event := <-eventsCh
		log.Printf("Receive EventStore\nSequnce: %d\nTime: %s\nBody: %s\n", event.Sequence, event.Timestamp, event.Body)
	}

}
