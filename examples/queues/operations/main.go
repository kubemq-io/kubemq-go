package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
	"time"
)

func sendAndReceive() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceive").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue")).
		SetTags(map[string]string{
			"key1": "value1",
			"key2": "value2",
		}).
		SetPolicyDelaySeconds(1).
		SetPolicyExpirationSeconds(10).
		SetPolicyMaxReceiveCount(3).
		SetPolicyMaxReceiveQueue("dlq")
	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceive").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetAutoAck(true)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//AckAll - Acknowledge all messages
	//if err := msgs.AckAll(); err != nil {
	//	log.Fatal(err)
	//}

	//NackAll - Not Acknowledge all messages
	//if err := msgs.NAckAll(); err != nil {
	//	log.Fatal(err)
	//}

	// RequeueAll - Requeue all messages
	//if err := msgs.ReQueueAll("requeue-queue-channel"); err != nil {
	//	log.Fatal(err)
	//}

	for _, msg := range msgs.Messages {
		log.Println(msg.String())

		// Ack - Acknowledge message
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}

		// Nack - Not Acknowledge message
		//if err := msg.NAck(); err != nil {
		//	log.Fatal(err)
		//}

		// Requeue - Requeue message
		//if err := msg.ReQueue("requeue-queue-channel"); err != nil {
		//	log.Fatal(err)
		//}
	}

}
func sendAndReceiveWithVisibility() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceiveWithVisibility").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue - with visibility"))

	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceiveWithVisibility").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetVisibilitySeconds(2)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	for _, msg := range msgs.Messages {
		log.Println(msg.String())
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}
}

func sendAndReceiveWithVisibilityExpiration() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceiveWithVisibility").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue - with visibility"))

	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceiveWithVisibility").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetVisibilitySeconds(2)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	for _, msg := range msgs.Messages {
		log.Println(msg.String())
		log.Println("Received message, waiting 3 seconds before ack")
		time.Sleep(3 * time.Second)
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}

}
func sendAndReceiveWithVisibilityExtension() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceiveWithVisibility").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue - with visibility"))

	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceiveWithVisibility").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetVisibilitySeconds(2)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	for _, msg := range msgs.Messages {
		log.Println(msg.String())
		log.Println("Received message, waiting 1 seconds before ack")
		time.Sleep(1 * time.Second)
		log.Println("Extending visibility for 3 seconds and waiting 2 seconds before ack")
		if err := msg.ExtendVisibility(3); err != nil {
			log.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}
}
func main() {
	sendAndReceive()
	sendAndReceiveWithVisibility()
	sendAndReceiveWithVisibilityExpiration()
	sendAndReceiveWithVisibilityExtension()
}
