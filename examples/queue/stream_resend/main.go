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
	channel := "testing_queue_channel_resend"
	resendToChannel := "testing_queue_channel_destination"

	sendResult, err := sender.NewQueueMessage().
		SetChannel(channel).
		SetBody([]byte("queue-message-resend")).
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
	log.Println("resend to new queue")

	err = msg.Resend(resendToChannel)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("done")
	stream.Close()
	// checking the new channel
	stream = receiverA.NewStreamQueueMessage().SetChannel(resendToChannel)
	// get message from the queue
	msg, err = stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("MessageID: %s, Body: %s", msg.Id, string(msg.Body))
	log.Println("resend with new message")
	newMsg := receiverA.NewQueueMessage().SetChannel(channel).SetBody([]byte("new message"))
	err = stream.ResendWithNewMessage(newMsg)

	if err != nil {
		log.Fatal(err)
	}
	stream.Close()
	log.Println("checking again the old queue")
	stream = receiverA.NewStreamQueueMessage().SetChannel(channel)
	// get message from the queue
	msg, err = stream.Next(ctx, 5, 10)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("MessageID: %s, Body: %s", msg.Id, string(msg.Body))

	err = msg.Ack()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("ack and done")
}
