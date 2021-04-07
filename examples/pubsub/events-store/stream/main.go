package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClientA, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-stream-a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClientA.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "events_store"
	err = eventsStoreClientA.Subscribe(ctx, channel, "g1", func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver A - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	}, kubemq.StartFromFirstEvent())
	if err != nil {
		log.Fatal(err)
	}
	eventsStoreClientB, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-stream-b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClientA.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	err = eventsStoreClientB.Subscribe(ctx, channel, "g1", func(msg *kubemq.EventStoreReceive, err error) {
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("Receiver B - Event Store Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", msg.Id, msg.Channel, msg.Metadata, msg.Body)
		}
	}, kubemq.StartFromFirstEvent())
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(100 * time.Millisecond)
	var gracefulShutdown = make(chan os.Signal, 1)
	signal.Notify(gracefulShutdown, syscall.SIGTERM)
	signal.Notify(gracefulShutdown, syscall.SIGINT)
	signal.Notify(gracefulShutdown, syscall.SIGQUIT)
	streamSender, err := eventsStoreClientA.Stream(ctx, func(result *kubemq.EventStoreResult, err error) {
		if err != nil {
			log.Fatal(err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	counter := 0
	for {
		select {
		case <-time.After(time.Second):
			counter++
			err := streamSender(kubemq.NewEventStore().
				SetId("some-id").
				SetChannel(channel).
				SetMetadata("some-metadata").
				SetBody([]byte(fmt.Sprintf("hello kubemq - sending event stream %d", counter))))

			if err != nil {
				log.Fatal(err)
			}
		case <-gracefulShutdown:
			return
		}
	}
}
