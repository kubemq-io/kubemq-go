// Example: queries/send-query
//
// Demonstrates sending a query and receiving a response with data.
// Unlike commands, queries return a body payload in the response.
//
// Channel: go-queries.send-query
// Client ID: go-queries-send-query-client
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
		kubemq.WithClientId("go-queries-send-query-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queries.send-query"
	done := make(chan struct{})

	// Subscribe to handle queries and return data.
	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Query received: channel=%s body=%s\n", q.Channel, q.Body)
			resp := kubemq.NewQueryReply().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"result":"data","status":"ok"}`)).
				SetMetadata("ok").
				SetExecutedAt(time.Now())
			_ = client.SendQueryResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Query subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// Send a query and receive the response.
	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("fetch-data")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Query response: executed=%v body=%s\n", qResp.Executed, qResp.Body)

	<-done
}

// Expected output:
// Query received: channel=go-queries.send-query body=fetch-data
// Query response: executed=true body={"result":"data","status":"ok"}
