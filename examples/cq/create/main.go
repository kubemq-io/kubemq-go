package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func createCommandsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := commandsClient.Create(ctx, "commands.A"); err != nil {
		log.Fatal(err)
	}
}

func createQueriesChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queriesClient.Create(ctx, "queries.A"); err != nil {
		log.Fatal(err)
	}
}

func main() {
	createCommandsChannel()
	createQueriesChannel()
}
