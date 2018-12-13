package main

import (
	"context"
	"github.com/kubemq-io/go"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-event-client-id"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channel := "testing_event_channel"
	errCh := make(chan error)
	eventsCh, err := client.SubscribeToEvents(ctx, channel, "", errCh)
	if err != nil {
		log.Fatal(err)
	}
	err = client.E().
		SetId("some-id").
		SetChannel(channel).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending single event")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		eventStreamCh := make(chan *kubemq.Event, 1)
		errStreamCh := make(chan error, 1)
		go client.StreamEvents(ctx, eventStreamCh, errStreamCh)
		event := client.E().SetId("some-event-id").
			SetChannel(channel).
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending stream event"))
		for {
			select {
			case err := <-errStreamCh:
				log.Println(err)
				return
			case eventStreamCh <- event:
				return
			}
		}

	}()

	for {
		select {
		case err := <-errCh:
			log.Fatal(err)
			return
		case event := <-eventsCh:
			log.Printf("Event Recevied:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
		}
	}

}
