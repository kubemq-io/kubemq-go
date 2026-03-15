// Example: queries/consumer-group
//
// Demonstrates load-balanced query handling with consumer groups.
// Multiple handlers in the same group share the query workload.
//
// Channel: go-queries.consumer-group
// Client ID: go-queries-consumer-group-client
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
		kubemq.WithClientId("go-queries-consumer-group-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queries.consumer-group"
	group := "go-queries-reader-group"
	done := make(chan struct{})

	// Subscribe with a consumer group.
	sub, err := client.SubscribeToQueries(ctx, channel, group,
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Worker received: body=%s\n", q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("group-result")).
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

	// Send a query to the group.
	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("group-fetch")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Group response: executed=%v body=%s\n", qResp.Executed, qResp.Body)

	<-done
}
