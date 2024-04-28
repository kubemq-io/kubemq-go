package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := commandsClient.Delete(ctx, "commands.A"); err != nil {
		log.Fatal(err)
	}
}
