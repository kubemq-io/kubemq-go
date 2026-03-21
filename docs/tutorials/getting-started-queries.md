# Getting Started with Queries

This tutorial walks you through building a request-reply data retrieval system with KubeMQ queries in Go. By the end, you'll have a query handler that responds with data and a client that sends queries and receives responses — plus optional response caching.

## What You'll Build

A request-reply system where one side sends a query (e.g., "get user list") and the other responds with data. Queries are different from commands: queries return a body payload in the response, while commands typically return only success/failure. This makes queries ideal for data retrieval, lookups, and read operations in a CQRS-style architecture.

## Prerequisites

- **Go 1.21 or later** installed ([download](https://go.dev/dl/))
- **KubeMQ server** running on `localhost:50000`. The quickest way:
  ```bash
  docker run -d -p 50000:50000 -p 9090:9090 kubemq/kubemq
  ```
- A new Go module for your project:
  ```bash
  mkdir kubemq-queries-tutorial && cd kubemq-queries-tutorial
  go mod init kubemq-queries-tutorial
  ```

## Step 1: Install the SDK

Pull the KubeMQ Go SDK into your module:

```bash
go get github.com/kubemq-io/kubemq-go/v2
```

This single dependency gives you access to events, queues, commands, and queries — all over a single gRPC connection.

## Step 2: Create a Query Handler

Create a file called `main.go`. We'll start with the handler because it needs to be listening *before* queries are sent — otherwise the query will time out waiting for a response.

```go
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

	// Connect to the KubeMQ server.
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("queries-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.queries"
	done := make(chan struct{})

	// Register a query handler.
	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("[Handler] Handling query: id=%s body=%s\n", q.Id, q.Body)

			// Process the query and prepare a response with data.
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"users":["alice","bob"]}`)).
				SetMetadata("success").
				SetExecutedAt(time.Now())
			if err := client.SendResponse(ctx, resp); err != nil {
				log.Printf("Failed to send response: %v", err)
			}
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Handler] Error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	fmt.Println("[Handler] Listening for queries on channel:", channel)

	// ... query sender code will go here (Step 3) ...
}
```

Let's break down the key parts:

- **`SubscribeToQueries`** takes the channel name, an optional consumer group (empty string means no group), and callbacks. The query callback receives `*kubemq.QueryReceive` with the query's `Id`, `Body`, `ResponseTo`, and other fields.
- **`kubemq.NewResponse()`** builds the response. You must set `SetRequestId(q.Id)` and `SetResponseTo(q.ResponseTo)` so the server routes the response back to the correct sender.
- **`client.SendResponse`** sends the response. The sender's `SendQuery` call will unblock with this data.
- **`sub.Unsubscribe()`** cleans up the subscription when you're done. Pairing it with `defer` ensures it always runs.

## Step 3: Send a Query and Receive the Response

Now add the code to send a query and receive the response. The handler needs a moment to register with the server, so we add a brief sleep before sending:

```go
	// Give the handler time to register with the server.
	time.Sleep(300 * time.Millisecond)

	// Send a query and wait for the response.
	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("list-users")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Client] Response: executed=%v body=%s metadata=%s\n",
		qResp.Executed, qResp.Body, qResp.Metadata)

	<-done
```

A few things to notice:

- **`kubemq.NewQuery()`** creates a query builder. You set the channel, body (as `[]byte`), and a timeout. The sender blocks until the handler responds or the timeout expires.
- **`SendQuery`** returns a `*kubemq.QueryResponse` with `Executed` (whether a handler ran), `Body` (the response payload), `Metadata`, and `CacheHit` (see Step 4).
- The timeout is required — it prevents the caller from waiting forever if no handler is available.

## Step 4: Cached Queries

For read-heavy workloads, you can cache responses on the server. Set `SetCacheKey` and `SetCacheTTL` on the query. Subsequent queries with the same cache key return the cached response without invoking the handler — and `QueryResponse.CacheHit` will be `true`:

```go
	// First query: handler is called, result is cached for 60 seconds.
	q1Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("get-config")).
		SetTimeout(10*time.Second).
		SetCacheKey("config-v1").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("First query:  executed=%v cacheHit=%v body=%s\n",
		q1Resp.Executed, q1Resp.CacheHit, q1Resp.Body)

	// Second query with same cache key: served from cache (no handler call).
	q2Resp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("get-config")).
		SetTimeout(10*time.Second).
		SetCacheKey("config-v1").
		SetCacheTTL(60*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Second query: executed=%v cacheHit=%v body=%s\n",
		q2Resp.Executed, q2Resp.CacheHit, q2Resp.Body)
```

When `CacheHit` is `true`, the handler was not invoked — the server returned the cached response. Use this for idempotent read operations that don't change often.

## Complete Program

Here's the complete program assembled:

```go
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
		kubemq.WithClientId("queries-tutorial-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := "tutorial.queries"
	done := make(chan struct{})

	sub, err := client.SubscribeToQueries(ctx, channel, "",
		kubemq.WithOnQueryReceive(func(q *kubemq.QueryReceive) {
			fmt.Printf("[Handler] Handling query: id=%s body=%s\n", q.Id, q.Body)
			resp := kubemq.NewResponse().
				SetRequestId(q.Id).
				SetResponseTo(q.ResponseTo).
				SetBody([]byte(`{"users":["alice","bob"]}`)).
				SetMetadata("success").
				SetExecutedAt(time.Now())
			if err := client.SendResponse(ctx, resp); err != nil {
				log.Printf("Failed to send response: %v", err)
			}
			close(done)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("[Handler] Error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer sub.Unsubscribe()

	fmt.Println("[Handler] Listening for queries on channel:", channel)

	time.Sleep(300 * time.Millisecond)

	qResp, err := client.SendQuery(ctx, kubemq.NewQuery().
		SetChannel(channel).
		SetBody([]byte("list-users")).
		SetTimeout(10*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[Client] Response: executed=%v body=%s metadata=%s\n",
		qResp.Executed, qResp.Body, qResp.Metadata)

	<-done
}
```

Run it:

```bash
go run main.go
```

### Expected Output

```
[Handler] Listening for queries on channel: tutorial.queries
[Handler] Handling query: id=<query-id> body=list-users
[Client] Response: executed=true body={"users":["alice","bob"]} metadata=success
```

## Key Concepts

| Concept | What It Means |
|---|---|
| **Queries vs commands** | Queries return a body payload in the response; commands typically return only success/failure. Use queries for reads, commands for writes. |
| **Request-reply** | The sender blocks until the handler responds or the timeout expires. Synchronous, unlike events. |
| **Caching** | Set `CacheKey` and `CacheTTL` on the query. The server caches the response; subsequent queries with the same key get `CacheHit=true` without invoking the handler. |
| **Timeout** | Required on every query. Prevents indefinite blocking if no handler is available. |

## Next Steps

- **Cached queries**: Add `SetCacheKey` and `SetCacheTTL` for read-heavy workloads. See [`examples/queries/cached-query/`](../../examples/queries/cached-query/).
- **Consumer groups**: Use consumer groups for load-balanced query handling. See [`examples/queries/consumer-group/`](../../examples/queries/consumer-group/).
- **Commands**: For write operations that don't need a data payload in the response, see [Request-Reply with Commands](request-reply-with-commands.md).
- **CQRS**: Combine commands and queries in a CQRS architecture. See [Implementing CQRS with KubeMQ](../scenarios/microservice-cqrs.md).
