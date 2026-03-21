// Example: queries/cached-query
//
// Demonstrates query caching with CacheKey and CacheTTL.
// The first query triggers the handler; subsequent queries with the same
// cache key are served from cache (CacheHit=true) without calling the handler.
//
// Channel: go-queries.cached-query
// Client ID: go-queries-cached-query-client
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
		kubemq.WithClientId("go-queries-cached-query-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "go-queries.cached-query"
	handlerCalled := make(chan struct{}, 2)

	// Register a query handler.
	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("Handler called: body=%s\n", q.Body)
			resp := kubemq.NewQueryReply().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("cached-result")).
				SetExecutedAt(time.Now())
			_ = client.SendQueryResponse(ctx, resp)
			select {
			case handlerCalled <- struct{}{}:
			default:
			}
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("Subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// First query: handler is called, result is cached for 60 seconds.
	q1Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("cacheable-query")).
		SetTimeout(10*time.Second).
		SetCacheKey("my-cache-key").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("First query:  executed=%v cacheHit=%v body=%s\n",
		q1Resp.Executed, q1Resp.CacheHit, q1Resp.Body)
	<-handlerCalled

	// Second query with same cache key: served from cache (no handler call).
	q2Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("cacheable-query")).
		SetTimeout(10*time.Second).
		SetCacheKey("my-cache-key").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Second query: executed=%v cacheHit=%v body=%s\n",
		q2Resp.Executed, q2Resp.CacheHit, q2Resp.Body)
}
