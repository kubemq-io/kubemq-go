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
	channel := "testing_queue_channel_dead-letter-queue"
	deadLetterQueue := "dead-letter-queue"

	sendResult, err := sender.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("queue-message-with-dead-letter-queue")).
		SetPolicyMaxReceiveCount(3).
		SetPolicyMaxReceiveQueue(deadLetterQueue).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	receiverA, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-stream-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	//kubemq.WithUri("http://localhost:9090"),
	//kubemq.WithClientId("test-client-sender-id"),
	//kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)

	}
	defer receiverA.Close()

	go func() {
		stream := receiverA.NewStreamQueueMessage().SetChannel(channel)

		for i := 0; i < 3; i++ {
			// get message from the queue
			msg, err := stream.Next(ctx, 2, 10)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("MessageID: %s, Body: %s", msg.Id, string(msg.Body))
			log.Println("no ack for 2 sec ")
			time.Sleep(2000 * time.Millisecond)
		}
		stream.Close()
	}()

	time.Sleep(3 * time.Second)

	stream := sender.NewStreamQueueMessage().SetChannel(deadLetterQueue)

	// get message from the queue
	msg, err := stream.Next(ctx, 10, 60)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Dead-Letter Queue MessageID: %s, Body: %s", msg.Id, string(msg.Body))
	err = msg.Ack()
	if err != nil {
		log.Fatal(err)
	}
	stream.Close()
}
