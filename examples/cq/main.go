// Package main demonstrates Commands and Queries (RPC) with the KubeMQ Go SDK v2.
//
// This example covers: send/receive commands and queries, consumer groups,
// timeout handling, and query caching (CacheKey + CacheTTL).
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
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	demoCommands(ctx, client)
	demoCommandGroup(ctx, client)
	demoCommandTimeout(ctx, client)
	demoQueries(ctx, client)
	demoQueryGroup(ctx, client)
	demoQueryCache(ctx, client)
}

// demoCommands shows basic command send + subscribe + response flow.
func demoCommands(ctx context.Context, client *kubemq.Client) {
	done := make(chan struct{})
	sub, err := client.SubscribeToCommands(ctx, "demo-commands", "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("[Command] received: channel=%s body=%s\n", cmd.Channel, cmd.Body)
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetBody([]byte("executed")).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) { log.Println("cmd sub error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("demo-commands").
		SetBody([]byte("do-something")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Command] response: executed=%v\n", cmdResp.Executed)
	<-done
}

// demoCommandGroup shows subscribe to commands with a consumer group.
func demoCommandGroup(ctx context.Context, client *kubemq.Client) {
	done := make(chan struct{})
	sub, err := client.SubscribeToCommands(ctx, "demo-cmd-group", "worker-group",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("[CmdGroup] worker received: body=%s\n", cmd.Body)
			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) { log.Println("cmd group error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	cmdResp, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("demo-cmd-group").
		SetBody([]byte("group-task")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[CmdGroup] response: executed=%v\n", cmdResp.Executed)
	<-done
}

// demoCommandTimeout shows handling a command timeout when no handler responds.
func demoCommandTimeout(ctx context.Context, client *kubemq.Client) {
	_, err := client.SendCommand(ctx, kubemq.NewCommand().
		SetChannel("demo-no-handler").
		SetBody([]byte("will timeout")).
		SetTimeout(2*time.Second))
	if err != nil {
		fmt.Printf("[Timeout] command timed out as expected: %v\n", err)
	} else {
		fmt.Println("[Timeout] unexpectedly succeeded")
	}
}

// demoQueries shows basic query send + subscribe + response flow.
func demoQueries(ctx context.Context, client *kubemq.Client) {
	done := make(chan struct{})
	sub, err := client.SubscribeToQueries(ctx, "demo-queries", "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("[Query] received: channel=%s body=%s\n", q.Channel, q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"result":"data"}`)).
				SetMetadata("ok").
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) { log.Println("query sub error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel("demo-queries").
		SetBody([]byte("fetch-data")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Query] response: executed=%v body=%s\n", qResp.Executed, qResp.Body)
	<-done
}

// demoQueryGroup shows subscribe to queries with a consumer group.
func demoQueryGroup(ctx context.Context, client *kubemq.Client) {
	done := make(chan struct{})
	sub, err := client.SubscribeToQueries(ctx, "demo-query-group", "reader-group",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("[QueryGroup] worker received: body=%s\n", q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("group-result")).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			close(done)
		}),
		kubemq.WithOnError(func(err error) { log.Println("query group error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel("demo-query-group").
		SetBody([]byte("group-fetch")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[QueryGroup] response: executed=%v body=%s\n", qResp.Executed, qResp.Body)
	<-done
}

// demoQueryCache shows query caching with CacheKey and CacheTTL.
func demoQueryCache(ctx context.Context, client *kubemq.Client) {
	done := make(chan struct{}, 1)
	sub, err := client.SubscribeToQueries(ctx, "demo-cache", "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("[Cache] handler called: body=%s\n", q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte("cached-result")).
				SetExecutedAt(time.Now())
			_ = client.SendResponse(ctx, resp)
			select {
			case done <- struct{}{}:
			default:
			}
		}),
		kubemq.WithOnError(func(err error) { log.Println("cache sub error:", err) }),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()
	time.Sleep(300 * time.Millisecond)

	// First query: handler is called, result is cached
	q1Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel("demo-cache").
		SetBody([]byte("cacheable-query")).
		SetTimeout(10*time.Second).
		SetCacheKey("my-cache-key").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Cache] first query: executed=%v cacheHit=%v body=%s\n", q1Resp.Executed, q1Resp.CacheHit, q1Resp.Body)
	<-done

	// Second query with same cache key: should be served from cache (CacheHit=true)
	q2Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel("demo-cache").
		SetBody([]byte("cacheable-query")).
		SetTimeout(10*time.Second).
		SetCacheKey("my-cache-key").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Cache] second query: executed=%v cacheHit=%v body=%s\n", q2Resp.Executed, q2Resp.CacheHit, q2Resp.Body)
}
