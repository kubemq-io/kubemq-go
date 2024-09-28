package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listCommandsChannels() {
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
	channels, err := commandsClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}

func listQueriesChannels() {
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
	channels, err := queriesClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}

func main() {
	listCommandsChannels()
	listQueriesChannels()
}
