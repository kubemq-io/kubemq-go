package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,

		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queuesClient.Create(ctx, "queues.A"); err != nil {
		log.Fatal(err)
	}
}
