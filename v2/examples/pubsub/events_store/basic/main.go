package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/v2/pubsub/events_store"
	"log"
	"runtime"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
	"github.com/kubemq-io/kubemq-go/v2/config"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// print current amount of go-routines
		for {
			time.Sleep(1 * time.Second)
			log.Printf("current go-routines: %d", runtime.NumGoroutine())
		}
	}()

	client, err := kubemq.NewClient(ctx, config.NewConnection().
		SetAddress("localhost:50000").
		SetClientId("test-events-client"))
	if err != nil {
		log.Fatal(err)
	}
	subscribeReq := events_store.NewEventsEventSubscription().
		SetChannel("es1").
		SetGroup("").
		SetStartAt(events_store.StartAtTypeFromNew).
		SetOnReceiveEvent(func(event *events_store.EventStoreReceived) {
			log.Printf("Received Event Store From Client: %s, Message: %s, Seq: %d, Timestamp: %s", event.FromClientId, event.Body, event.Sequence, event.Timestamp)
		}).SetOnError(func(err error) {
		log.Printf("error received, error: %s", err.Error())
	})
	err = client.SubscribeToEventsStore(ctx, subscribeReq)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second)
	err = client.SendEventStore(ctx, events_store.NewEventStore().
		SetChannel("es1").
		SetBody([]byte("hello kubemq - sending event store")))
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Hour)
}
