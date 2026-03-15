// Example: commands/handle-command
//
// Demonstrates subscribing to commands and handling them with business logic.
// The handler processes incoming commands and sends back responses.
//
// Channel: go-commands.handle-command
// Client ID: go-commands-handle-command-client
//
// Run with a KubeMQ server on localhost:50000
// (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kubemq-io/kubemq-go/v2"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-commands-handle-command-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-commands.handle-command"
	done := make(chan struct{})

	// Register a command handler that processes incoming commands.
	sub, err := client.SubscribeToCommands(ctx, channel, "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("Handling command: id=%s body=%s metadata=%s\n",
				cmd.Id, cmd.Body, cmd.Metadata)

			// Process the command (business logic goes here).
			// Then send back a response.
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now())
			if err := client.SendResponse(ctx, resp); err != nil {
				log.Printf("Failed to send response: %v", err)
			}
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Handler error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// Send a command to trigger the handler.
	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel(channel).
		SetBody([]byte("process-order")).
		SetMetadata("order-123").
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Response: executed=%v\n", cmdResp.Executed)

	<-done
}
