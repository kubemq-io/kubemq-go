package main

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-client-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer sender.Close()
	channel := "testing_queue_channel"

	workerA, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-client-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer workerA.Close()
	workerB, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-client-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)
	}
	defer workerB.Close()

	batch := sender.NewQueueMessages()
	for i := 0; i < 10; i++ {
		batch.Add(sender.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("Batch Message %d", i))).
			SetPolicyExpirationSeconds(2))

	}
	batchResult, err := batch.Send(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, sendResult := range batchResult {
		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}
	// waiting for 5 seconds
	time.Sleep(5 * time.Second)

	go func() {
		for {
			receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(10).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker A Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
			log.Printf("Worker A notified of %d Expired Messages:\n", receiveResult.MessagesExpired)

		}

	}()

	go func() {
		for {
			receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
				SetChannel(channel).
				SetMaxNumberOfMessages(10).
				SetWaitTimeSeconds(2).
				Send(ctx)
			if err != nil {
				return
			}
			log.Printf("Worker B Received %d Messages:\n", receiveResult.MessagesReceived)
			for _, msg := range receiveResult.Messages {
				log.Printf("MessageID: %s, Body: %s", msg.MessageID, string(msg.Body))
			}
			log.Printf("Worker B notified of %d Expired Messages:\n", receiveResult.MessagesExpired)
		}

	}()
	time.Sleep(3 * time.Second)
}
