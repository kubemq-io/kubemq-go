// Example: queries/handle-query
//
// Demonstrates subscribing to queries and handling them with business logic.
// The handler processes incoming queries and returns data in the response.
//
// Channel: go-queries.handle-query
// Client ID: go-queries-handle-query-client
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
		kubemq.WithClientId("go-queries-handle-query-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queries.handle-query"
	done := make(chan struct{})

	// Register a query handler.
	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Handling query: id=%s body=%s\n", q.Id, q.Body)

			// Process the query and prepare a response.
			resp := kubemq.NewQueryReply().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"users":["alice","bob"]}`)).
				SetMetadata("success").
				SetExecutedAt(time.Now())
			if err := client.SendQueryResponse(ctx, resp); err != nil {
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

	// Send a query.
	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("list-users")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Response: executed=%v body=%s metadata=%s\n",
		qResp.Executed, qResp.Body, qResp.Metadata)

	<-done
}
