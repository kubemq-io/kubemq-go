package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := kubemq.NewQueuesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("go-sdk-cookbook-pubsub-events-store-stream-a"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channel := "queues.single"
	sendResult, err := queuesClient.Send(ctx, kubemq.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("some-simple_queue-queue-message")))

	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	qt, err := queuesClient.Transaction(ctx, kubemq.NewQueueTransactionMessageRequest().
		SetChannel(channel).
		SetWaitTimeSeconds(5).
		SetVisibilitySeconds(10))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s Received, Body: %s", qt.Message.MessageID, string(qt.Message.Body))
	time.Sleep(1 * time.Second)
	err = qt.Resend("queue.2")
	if err != nil {
		log.Fatal(err)
	}

}
