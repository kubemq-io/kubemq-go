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
		kubemq.WithClientId("test-event-client-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channelName := "testing_event_channel"

	go func() {
		errCh := make(chan error)
		eventsCh, err := client.SubscribeToEvents(ctx, channelName, "", errCh)
		if err != nil {
			log.Fatal(err)
			return

		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case event, more := <-eventsCh:
				if !more {
					fmt.Println("Event Received, done")
					return
				}
				log.Printf("Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
			case <-ctx.Done():
				return
			}
		}
	}()
	// wait for receiver ready
	time.Sleep(100 * time.Millisecond)
	err = client.E().
		SetId("some-id").
		SetChannel(channelName).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending single event")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	eventStreamCh := make(chan *kubemq.Event, 1)
	errStreamCh := make(chan error, 1)
	go client.StreamEvents(ctx, eventStreamCh, errStreamCh)
	event := client.E().SetId("some-event-id").
		SetChannel(channelName).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending stream event"))
	select {
	case err := <-errStreamCh:
		log.Println(err)
	case eventStreamCh <- event:
	}
	time.Sleep(1 * time.Second)
}
