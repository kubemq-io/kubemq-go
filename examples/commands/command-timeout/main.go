// Example: commands/command-timeout
//
// Demonstrates command timeout handling. When no handler responds
// within the timeout period, the SendCommand call returns an error.
//
// Channel: go-commands.command-timeout
// Client ID: go-commands-command-timeout-client
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
		kubemq.WithClientId("go-commands-command-timeout-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Send a command to a channel with no handler — it will timeout.
	_, err = client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("go-commands.command-timeout").
		SetBody([]byte("will timeout")).
		SetTimeout(2*time.Second))
	if err != nil {
		fmt.Printf("Command timed out as expected: %v\n", err)
	} else {
		fmt.Println("Unexpected: command succeeded without a handler")
	}
}
