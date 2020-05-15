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

	receiver, err := kubemq.NewClient(ctx,
		kubemq.WithUri("http://localhost:9090"),
		kubemq.WithClientId("test-stream-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver.Close()
	channel := "testing_queue_channel"
	sendResult, err := sender.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("some-stream_simple-queue-message")).
		Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())

	stream := receiver.NewStreamQueueMessage().SetChannel(channel)

	// get message from the queue
	msg, err := stream.Next(ctx, 10, 10)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
	log.Println("doing some work.....")
	time.Sleep(time.Second)
	// ack the current message
	log.Println("done, ack the message")
	err = msg.Ack()
	if err != nil {
		log.Fatal(err)
	}

	log.Println("checking for next message")
	msg, err = stream.Next(ctx, 10, 1)
	if err != nil {
		log.Println(err.Error())
	}
	if msg == nil {
		log.Println("no new message in the queue")
	}
	stream.Close()
}
