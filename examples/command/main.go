package main

import (
	"context"
	"github.com/kubemq-io/go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-command-client-id"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channel := "testing_command_channel"

	go func() {
		errCh := make(chan error)
		commandsCh, err := client.SubscribeToCommands(ctx, channel, "", errCh)
		if err != nil {
			log.Fatal(err)
		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case command := <-commandsCh:
				log.Printf("Command Recevied:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", command.Id, command.Channel, command.Metadata, command.Body)
				err := client.R().
					SetRequestId(command.Id).
					SetResponseTo(command.ResponseTo).
					SetExecutedAt(time.Now()).
					Send(ctx)
				if err != nil {
					log.Fatal(err)
				}
			case <-ctx.Done():
				return
			}
		}

	}()
	// give some time to connect a receiver
	time.Sleep(time.Second)
	response, err := client.C().
		SetId("some-command-id").
		SetChannel(channel).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending command, please reply")).
		SetTimeout(time.Second).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Resonse Recevied:\nCommandID: %s\nExecutedAt:%s\n", response.CommandId, response.ExecutedAt)

}
