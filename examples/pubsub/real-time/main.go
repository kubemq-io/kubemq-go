package real_time

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-event-grpc-client"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer sender.Close()
	receiverA, err := kubemq.NewClient(ctx,
		kubemq.WithUri("http://localhost:9090"),
		kubemq.WithClientId("test-event-rest-client"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)
	}

	defer receiverA.Close()

	receiverB, err := kubemq.NewClient(ctx,
		kubemq.WithUri("http://localhost:9090"),
		kubemq.WithClientId("test-event-rest-client"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)
	}

	defer receiverB.Close()

	channelName := "testing_event_channel"
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverA.SubscribeToEvents(ctx, channelName, "", errCh)
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
					log.Println("Receiver A - Event Received, done")
					return
				}
				log.Printf("Receiver A - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		errCh := make(chan error)
		eventsCh, err := receiverB.SubscribeToEvents(ctx, channelName, "", errCh)
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
					log.Println("Receiver B - Event Received, done")
					return
				}
				log.Printf("Receiver B - Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
			case <-ctx.Done():
				return
			}
		}
	}()
	// wait for receiver ready
	time.Sleep(100 * time.Millisecond)
	err = sender.E().
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
	go sender.StreamEvents(ctx, eventStreamCh, errStreamCh)
	event := sender.E().SetId("some-event-id").
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
