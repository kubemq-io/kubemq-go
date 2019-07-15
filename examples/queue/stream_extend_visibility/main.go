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
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-stream-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer sender.Close()
	channel := "testing_queue_channel_visibility"

	sendResult, err := sender.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("queue-message-with-for-extend-visibility")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	receiverA, err := kubemq.NewClient(ctx,
		kubemq.WithUri("http://localhost:9090"),
		kubemq.WithClientId("test-client-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)

	}
	defer receiverA.Close()

	stream := receiverA.NewStreamQueueMessage().SetChannel(channel)

	// get message from the queue
	msg, err := stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s, Body: %s", msg.Id, string(msg.Body))
	log.Println("work for 1 seconds")
	time.Sleep(1000 * time.Millisecond)
	log.Println("need more time to process, extend visibility for more 3 seconds")
	err = msg.ExtendVisibility(3)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("approved. work for 2.5 seconds")
	time.Sleep(2500 * time.Millisecond)
	log.Println("work done.... ack the message")
	err = msg.Ack()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("ack done")

	stream.Close()

}
