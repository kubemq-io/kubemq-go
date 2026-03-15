// Example: commands/consumer-group
//
// Demonstrates load-balanced command handling with consumer groups.
// Multiple handlers in the same group share the command workload,
// with each command delivered to exactly one handler.
//
// Channel: go-commands.consumer-group
// Client ID: go-commands-consumer-group-client
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
		kubemq.WithClientId("go-commands-consumer-group-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-commands.consumer-group"
	group := "go-commands-worker-group"
	done := make(chan struct{})

	// Subscribe with a consumer group for load-balanced command handling.
	sub, err := client.SubscribeToCommands(ctx, channel, group,
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("Worker received: body=%s\n", cmd.Body)
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Group error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// Send a command to the group.
	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel(channel).
		SetBody([]byte("group-task")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Group response: executed=%v\n", cmdResp.Executed)

	<-done
}
