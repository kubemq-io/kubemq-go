package kubemq_test

import (
	"context"
	"fmt"
	"log"
	"time"

	kubemq "github.com/kubemq-io/kubemq-go/v2"
)

func ExampleNewClient() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example-client"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("client created")
	// Output: client created
}

func ExampleNewClient_withOptions() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("kubemq-server.example.com", 50000),
		kubemq.WithClientId("my-service"),
		kubemq.WithConnectionTimeout(10*time.Second),
		kubemq.WithAuthToken("my-auth-token"),
		kubemq.WithCheckConnection(false),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	fmt.Println("client created with options")
	// Output: client created with options
}

func ExampleClient_SendEvent() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("event-sender"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	event := kubemq.NewEvent().
		SetChannel("events.notifications").
		SetMetadata("text/plain").
		SetBody([]byte("user signed up")).
		AddTag("source", "auth-service")

	err = client.SendEvent(ctx, event)
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_SubscribeToEvents() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("event-subscriber"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sub, err := client.SubscribeToEvents(ctx, "events.notifications", "",
		kubemq.WithOnEvent(func(event *kubemq.Event) {
			fmt.Printf("received: channel=%s body=%s\n", event.Channel, event.Body)
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Block until done, then clean up.
	<-ctx.Done()
	sub.Unsubscribe()
}

func ExampleClient_SendEventStore() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("eventstore-sender"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	event := kubemq.NewEventStore().
		SetChannel("events-store.orders").
		SetMetadata("application/json").
		SetBody([]byte(`{"orderId":"12345","status":"created"}`))

	result, err := client.SendEventStore(ctx, event)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("sent=%v id=%s\n", result.Sent, result.Id)
}

func ExampleClient_SendQueueMessage() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("queue-sender"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	msg := kubemq.NewQueueMessage().
		SetChannel("queues.tasks").
		SetBody([]byte("process-invoice")).
		SetMetadata("task/invoice").
		SetMaxReceiveCount(3).
		SetMaxReceiveQueue("queues.dead-letter")

	result, err := client.SendQueueMessage(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("sent=%v messageId=%s\n", !result.IsError, result.MessageID)
}

func ExampleClient_SubscribeToCommands() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("command-handler"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	sub, err := client.SubscribeToCommands(ctx, "commands.user", "",
		kubemq.WithOnCommandReceive(func(cmd *kubemq.CommandReceive) {
			fmt.Printf("command received: id=%s body=%s\n", cmd.Id, cmd.Body)

			resp := kubemq.NewResponse().
				SetRequestId(cmd.Id).
				SetResponseTo(cmd.ResponseTo).
				SetExecutedAt(time.Now())

			if err := client.SendResponse(ctx, resp); err != nil {
				log.Println("failed to send response:", err)
			}
		}),
		kubemq.WithOnError(func(err error) {
			log.Println("command subscription error:", err)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	sub.Unsubscribe()
}

func ExampleClient_SendCommand() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("command-sender"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	cmd := kubemq.NewCommand().
		SetChannel("commands.user").
		SetBody([]byte("delete-inactive-users")).
		SetMetadata("admin-action").
		SetTimeout(10 * time.Second)

	response, err := client.SendCommand(ctx, cmd)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("executed=%v at=%s\n", response.Executed, response.ExecutedAt)
}

func ExampleClient_SendQuery() {
	ctx := context.Background()

	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("query-sender"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	query := kubemq.NewQuery().
		SetChannel("queries.users").
		SetBody([]byte(`{"userId":"42"}`)).
		SetMetadata("application/json").
		SetTimeout(5 * time.Second).
		SetCacheKey("user-42").
		SetCacheTTL(time.Minute)

	response, err := client.SendQuery(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("executed=%v cacheHit=%v body=%s\n",
		response.Executed, response.CacheHit, response.Body)
}
