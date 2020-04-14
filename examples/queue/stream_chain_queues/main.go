package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/kubemq-io/kubemq-go"
	"github.com/nats-io/nuid"
	"log"
	"sync"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	host := "localhost"
	port := 32707
	uri := "http://localhost:30830"
	doneCh := "done"
	deadCh := "dead"
	sendCount := 10
	sender, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-stream-sender-id"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer sender.Close()

	receiver1, err := kubemq.NewClient(ctx,
		kubemq.WithUri(uri),
		kubemq.WithClientId("test-client-sender-id_receiver_a"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver1.Close()

	receiver2, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver2.Close()

	receiver3, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_c"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver3.Close()

	receiver4, err := kubemq.NewClient(ctx,
		kubemq.WithUri(uri),
		kubemq.WithClientId("test-client-sender-id_receiver_a"),
		kubemq.WithTransportType(kubemq.TransportTypeRest))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver4.Close()

	receiver5, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_b"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver5.Close()

	receiver6, err := kubemq.NewClient(ctx,
		kubemq.WithAddress(host, port),
		kubemq.WithClientId("test-client-sender-id_receiver_c"),
		kubemq.WithTransportType(kubemq.TransportTypeGRPC))
	if err != nil {
		log.Fatal(err)

	}
	defer receiver6.Close()

	wg := sync.WaitGroup{}
	wg.Add(6)

	for i := 1; i <= sendCount; i++ {
		messageID := nuid.New().Next()
		sendResult, err := sender.NewQueueMessage().
			SetId(messageID).
			SetChannel("receiverA").
			SetBody([]byte(fmt.Sprintf("sending message %d", i))).
			Send(ctx)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Send to Queue Result: MessageID:%s,Sent At: %s\n", sendResult.MessageID, time.Unix(0, sendResult.SentAt).String())
	}

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		for {
			stream := receiver1.NewStreamQueueMessage().SetChannel("receiverA")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverA")
				return
			}
			log.Printf("Queue: ReceiverA,MessageID: %s, Body: %s,Seq: %d - send to queue receiverB", msg.Id, string(msg.Body), msg.Attributes.Sequence)
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("receiverB")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
		}

	}()

	go func() {
		defer wg.Done()
		for {

			stream := receiver2.NewStreamQueueMessage().SetChannel("receiverB")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverB")
				return
			}
			log.Printf("Queue: ReceiverB,MessageID: %s, Body: %s - send to new receiverC", msg.Id, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("receiverC")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}

	}()

	go func() {
		defer wg.Done()
		for {
			stream := receiver3.NewStreamQueueMessage().SetChannel("receiverC")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverC")
				return
			}
			log.Printf("Queue: ReceiverC,MessageID: %s, Body: %s - send to new receiverD", msg.Id, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("receiverD")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
		}

	}()
	go func() {
		defer wg.Done()
		for {
			stream := receiver4.NewStreamQueueMessage().SetChannel("receiverD")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverD")
				return
			}
			log.Printf("Queue: ReceiverD MessageID: %s, Body: %s - send to queue receiverE", msg.Id, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("receiverE")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
		}

	}()

	go func() {
		defer wg.Done()
		for {
			stream := receiver5.NewStreamQueueMessage().SetChannel("receiverE")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverE")
				return
			}
			log.Printf("Queue: ReceiverE,MessageID: %s, Body: %s - send to new receiverF", msg.Id, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel("receiverF")
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Fatal(err)
			}
		}

	}()

	go func() {
		defer wg.Done()

		for {
			stream := receiver6.NewStreamQueueMessage().SetChannel("receiverF")
			// get message from the queue
			msg, err := stream.Next(ctx, 3, 5)
			if err != nil {
				log.Println("No new messages for ReceiverF")
				return
			}
			log.Printf("Queue: ReceiverF,MessageID: %s, Body: %s - send to sender done", msg.Id, string(msg.Body))
			time.Sleep(10 * time.Millisecond)
			msg.SetChannel(doneCh)
			err = stream.ResendWithNewMessage(msg)
			if err != nil {
				log.Println("receiverE", "err", err.Error())
				continue
			}
		}

	}()
	wg.Wait()
	res, err := sender.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		RequestID:           "some_request",
		ClientID:            "sender-client_id",
		Channel:             doneCh,
		MaxNumberOfMessages: 1000,
		WaitTimeSeconds:     2,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.IsError {
		log.Fatal(res.Error)
	}
	log.Printf("Done Messages - %d: Expried - %d", res.MessagesReceived, res.MessagesExpired)
	for i := 0; i < len(res.Messages); i++ {
		log.Printf("MessageID: %s, Body: %s, Seq: %d", res.Messages[i].Id, res.Messages[i].Body, res.Messages[i].Attributes.Sequence)
	}

	res, err = sender.ReceiveQueueMessages(ctx, &kubemq.ReceiveQueueMessagesRequest{
		RequestID:           "some_request",
		ClientID:            "sender-client_id",
		Channel:             deadCh,
		MaxNumberOfMessages: int32(sendCount),
		WaitTimeSeconds:     2,
		IsPeak:              false,
	})
	if err != nil {
		log.Fatal(err)
	}
	if res.IsError {
		log.Fatal(res.Error)
	}
	log.Printf("Dead Letter Messages - %d: Expried - %d", res.MessagesReceived, res.MessagesExpired)
	for i := 0; i < len(res.Messages); i++ {
		log.Printf("MessageID: %s, Body: %s", res.Messages[i].Id, res.Messages[i].Body)
	}
}
