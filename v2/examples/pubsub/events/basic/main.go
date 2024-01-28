package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/v2/pubsub/events"
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
	subscribeReq := events.NewEventsEventSubscription().
		SetChannel("e1").
		SetGroup("").
		SetOnReceiveEvent(func(event *events.EventReceived) {
			log.Printf("Received Event From Client: %s, Message: %s, Timestamp: %s", event.FromClientId, event.Body, event.Timestamp)
		}).SetOnError(func(err error) {
		log.Printf("error received, error: %s", err.Error())
	})
	err = client.SubscribeToEvents(ctx, subscribeReq)
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(time.Second)
	err = client.SendEvent(ctx, events.NewEvent().
		SetChannel("e1").
		SetBody([]byte("hello kubemq - sending event")))
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Hour)
}
