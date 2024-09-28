package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendReceiveCommands() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("sendReceiveCommands"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subRequest := &kubemq.CommandsSubscription{
		Channel:  "commands",
		ClientId: "",
		Group:    "",
	}
	log.Println("subscribing to commands")
	err = commandsClient.Subscribe(ctx, subRequest, func(cmd *kubemq.CommandReceive, err error) {
		log.Println(cmd.String())
		resp := &kubemq.Response{
			RequestId:  cmd.Id,
			ResponseTo: cmd.ResponseTo,
			Metadata:   "some-metadata",
			ExecutedAt: time.Now(),
		}
		if err := commandsClient.Response(ctx, resp); err != nil {
			log.Fatal(err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	log.Println("sending command")
	result, err := commandsClient.Send(ctx, kubemq.NewCommand().
		SetChannel("commands").
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending command")).
		SetTimeout(time.Duration(10)*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}

func sendReceiveQueries() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("sendReceiveQueries"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subRequest := &kubemq.QueriesSubscription{
		Channel:  "queries",
		ClientId: "",
		Group:    "",
	}
	log.Println("subscribing to queries")
	err = queriesClient.Subscribe(ctx, subRequest, func(query *kubemq.QueryReceive, err error) {
		log.Println(query.String())
		resp := &kubemq.Response{
			RequestId:  query.Id,
			ResponseTo: query.ResponseTo,
			Metadata:   "some-metadata",
			ExecutedAt: time.Now(),
			Body:       []byte("hello kubemq - sending query response"),
		}
		if err := queriesClient.Response(ctx, resp); err != nil {
			log.Fatal(err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	log.Println("sending query")
	result, err := queriesClient.Send(ctx, kubemq.NewQuery().
		SetChannel("queries").
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending query")).
		SetTimeout(time.Duration(10)*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}
func main() {
	sendReceiveCommands()
	sendReceiveQueries()
}
