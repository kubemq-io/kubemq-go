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
	for i := 0; i < 200; i++ {
		batch.Add(sender.NewQueueMessage().
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("Batch Message %d", i))).
			SetPolicyDelaySeconds(5).AddTag("message", fmt.Sprintf("%d", i)))

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
	//
	//go func() {
	//	for {
	//		receiveResult, err := workerA.NewReceiveQueueMessagesRequest().
	//			SetChannel(channel).
	//			SetMaxNumberOfMessages(1).
	//			SetWaitTimeSeconds(10).
	//			Send(ctx)
	//		if err != nil {
	//			return
	//		}
	//
	//		log.Printf("Worker A Received %d Messages at %s: ", receiveResult.MessagesReceived, time.Now().String())
	//		for _, msg := range receiveResult.Messages {
	//			log.Printf("MessageID: %s, Body: %s\n", msg.Id, string(msg.Body))
	//		}
	//
	//	}
	//
	//}()
	//
	//go func() {
	//	for {
	//
	//		receiveResult, err := workerB.NewReceiveQueueMessagesRequest().
	//			SetChannel(channel).
	//			SetMaxNumberOfMessages(1).
	//			SetWaitTimeSeconds(10).
	//			Send(ctx)
	//		if err != nil {
	//			return
	//		}
	//
	//		log.Printf("Worker B Received %d Messages at %s: ", receiveResult.MessagesReceived, time.Now().String())
	//		for _, msg := range receiveResult.Messages {
	//			log.Printf("MessageID: %s, Body: %s\n", msg.Id, string(msg.Body))
	//		}
	//	}
	//
	//}()
	//time.Sleep(4 * time.Second)
}
