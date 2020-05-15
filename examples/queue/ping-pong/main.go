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
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("167.99.7.222", 30888),
		kubemq.WithClientId("test-command-client-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channel := "q1"
	for {
		sendResult, err := client.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte("some-simple_queue-queue-message")).
			Send(ctx)
		if err != nil {
			log.Println(err)
		}
		if sendResult != nil {
			log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
		}

		receiveResult, err := client.NewReceiveQueueMessagesRequest().
			SetChannel(channel).
			SetMaxNumberOfMessages(10).
			SetWaitTimeSeconds(1).
			Send(ctx)
		if err != nil {
			log.Println(err)
		}
		if receiveResult != nil {
			log.Printf("Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("Seq: %d,MessageID: %s, Body: %s", msg.Attributes.Sequence, msg.MessageID, string(msg.Body))
			}
		}

	}

}
