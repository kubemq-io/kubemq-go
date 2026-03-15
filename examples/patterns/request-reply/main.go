// Example: patterns/request-reply
//
// Demonstrates the request-reply pattern using queries.
// A client sends a query and waits for a response from a handler.
// This is the fundamental RPC pattern in KubeMQ.
//
// Channel: go-patterns.request-reply
// Client ID: go-patterns-request-reply-client
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
		kubemq.WithClientId("go-patterns-request-reply-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-patterns.request-reply"
	done := make(chan struct{})

	// Service: subscribe to handle requests.
	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Service received request: body=%s\n", q.Body)

			// Process the request and return a reply.
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"order_id":"ORD-123","status":"confirmed"}`)).
				SetMetadata("success").
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Service error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// Client: send a request and wait for the reply.
	reply, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte(`{"action":"create_order","item":"widget"}`)).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Client received reply: executed=%v body=%s\n",
		reply.Executed, reply.Body)

	<-done
}
