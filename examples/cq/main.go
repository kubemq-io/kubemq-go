// Package main demonstrates Commands and Queries (RPC) with the KubeMQ Go SDK v2.
//
// This example sends a command, handles it with a response, and sends a query.
// Run with a KubeMQ server on localhost:50000 (e.g., docker run -d -p 50000:50000 kubemq/kubemq).
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

	// Create a KubeMQ client
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Start command handler in background
	cmdDone := make(chan struct{})
	sub, err := client.SubscribeToCommands(ctx, "demo-commands", "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("Received command: channel=%s body=%s\n", cmd.Channel, cmd.Body)
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetMetadata("ack").
				SetBody([]byte("command executed")).
				SetExecutedAt(time.Now())
			if err := client.SendResponse(ctx, resp); err != nil {
				log.Println("SendResponse error:", err)
			}
			close(cmdDone)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	// Give handler time to subscribe
	time.Sleep(500 * time.Millisecond)

	// Send a command
	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("demo-commands").
		SetBody([]byte("hello command")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Command response: executed=%v error=%s\n", cmdResp.Executed, cmdResp.Error)

	// Wait for handler to process
	select {
	case <-cmdDone:
	case <-ctx.Done():
		log.Println("Timed out waiting for command handler")
	}

	// Query example: start query handler
	queryDone := make(chan struct{})
	qSub, err := client.SubscribeToQueries(ctx, "demo-queries", "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Received query: channel=%s body=%s\n", q.Channel, q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetMetadata("ok").
				SetBody([]byte("query result")).
				SetExecutedAt(time.Now())
			if err := client.SendResponse(ctx, resp); err != nil {
				log.Println("SendResponse error:", err)
			}
			close(queryDone)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Query subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer qSub.Unsubscribe()

	time.Sleep(500 * time.Millisecond)

	// Send a query
	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel("demo-queries").
		SetBody([]byte("fetch data")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Query response: executed=%v body=%s\n", qResp.Executed, qResp.Body)

	select {
	case <-queryDone:
	case <-ctx.Done():
		log.Println("Timed out waiting for query handler")
	}
}
