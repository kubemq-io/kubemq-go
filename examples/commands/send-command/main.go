// Example: commands/send-command
//
// Demonstrates sending a command (RPC-style request) and receiving a response.
// A handler subscribes to the command channel, processes the command,
// and sends back an execution response.
//
// Channel: go-commands.send-command
// Client ID: go-commands-send-command-client
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
		kubemq.WithClientId("go-commands-send-command-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-commands.send-command"
	done := make(chan struct{})

	// Subscribe to handle incoming commands.
	sub, err := client.SubscribeToCommands(ctx, channel, "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("Command received: channel=%s body=%s\n", cmd.Channel, cmd.Body)
			// Send a response indicating successful execution.
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetBody([]byte("executed")).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Command subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond) // Allow subscription to establish.

	// Send a command and wait for the response.
	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel(channel).
		SetBody([]byte("do-something")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Command response: executed=%v\n", cmdResp.Executed)

	<-done
}

// Expected output:
// Command received: channel=go-commands.send-command body=do-something
// Command response: executed=true
