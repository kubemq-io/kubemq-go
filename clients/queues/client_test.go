package queues

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/clients/base"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"sync"
	"testing"
	"time"
)

func createBatchMessages(channel string, amount int) []*QueueMessage {
	var list []*QueueMessage
	for i := 0; i < amount; i++ {
		list = append(list, NewQueueMessage().
			SetId(uuid.New()).
			SetChannel(channel).
			SetBody([]byte(fmt.Sprintf("message %d", i))))
	}
	return list
}

func TestQueuesClient_Send(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	messages := createBatchMessages("queues.send", 10)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
}
func TestQueuesClient_Send_WithInvalid_Data(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	messages := createBatchMessages("", 10)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.True(t, messageResult.IsError)
	}
}
func TestQueuesClient_Send_Concurrent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	counter := *atomic.NewInt32(0)
	wg := sync.WaitGroup{}
	wg.Add(20)

	for i := 0; i < 20; i++ {
		go func(index int) {
			defer wg.Done()
			messages := createBatchMessages(fmt.Sprintf("queues.send.%d", index), 20000)
			result, err := queueClient.Send(ctx, messages...)
			require.NoError(t, err)
			require.EqualValues(t, 20000, len(result.Results))
			for _, messageResult := range result.Results {
				require.False(t, messageResult.IsError)
			}
			counter.Add(int32(len(result.Results)))
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, int32(400000), counter.Load())

}
func TestQueuesClient_Poll_AutoAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestQueuesClient_Poll_Long_AutoAck_ClientContextCancelled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(10000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.Error(t, err)
	require.Nil(t, response)
}

func TestQueuesClient_Poll_ManualAck_AckAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	err = response.AckAll()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	err = response.AckAll()

	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestQueuesClient_Poll_ManualAck_NAckAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}

	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))

	err = response.NAckAll()
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Error(t, response.NAckAll())

	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestQueuesClient_Poll_ManualAck_ReQueueAll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 10
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, 10, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)

	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	reQueueChannel := uuid.New()
	err = response.ReQueueAll(reQueueChannel)
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Error(t, response.ReQueueAll(reQueueChannel))

	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
	pollRequest3 := NewPollRequest().
		SetChannel(reQueueChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(ctx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response3.Messages))
}
func TestQueuesClient_Poll_ManualAck_AckRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	for i, msg := range response.Messages {
		err := msg.Ack()
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response2.Messages))
}
func TestQueuesClient_Poll_ManualAck_NAckRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	for i, msg := range response.Messages {
		err := msg.NAck()
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestQueuesClient_Poll_ManualAck_ReQueueRange(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	reQueueChannel := uuid.New()
	for i, msg := range response.Messages {
		err := msg.ReQueue(reQueueChannel)
		require.NoErrorf(t, err, "msg id %d", i)
	}
	pollRequest2 := NewPollRequest().
		SetChannel(reQueueChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
	pollRequest3 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(ctx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(response3.Messages))
}
func TestQueuesClient_Poll_ManualAck_Close(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	err = response.Close()
	require.NoError(t, err)
	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(ctx, pollRequest2)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response2.Messages))
}
func TestQueuesClient_Poll_ManualAck_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	time.Sleep(time.Second)
	cancel()

	pollRequest2 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response2, err := queueClient.Poll(context.Background(), pollRequest2)
	require.Error(t, err)
	require.Nil(t, response2)
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	queueClient, err = NewQueuesClient(newCtx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest3 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(newCtx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response3.Messages))
}
func TestQueuesClient_Poll_ManualAck_CloseClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	testChannel := uuid.New()
	messagesCount := 1000
	messages := createBatchMessages(testChannel, messagesCount)
	result, err := queueClient.Send(ctx, messages...)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(result.Results))
	for _, messageResult := range result.Results {
		require.False(t, messageResult.IsError)
	}
	pollRequest := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(false).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response.Messages))
	time.Sleep(time.Second)
	err = queueClient.Close()
	require.NoError(t, err)
	queueClient, err = NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest3 := NewPollRequest().
		SetChannel(testChannel).
		SetAutoAck(true).
		SetMaxItems(messagesCount).
		SetWaitTimeout(1000)
	response3, err := queueClient.Poll(ctx, pollRequest3)
	require.NoError(t, err)
	require.EqualValues(t, messagesCount, len(response3.Messages))
}
func TestQueuesClient_Poll_InvalidRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest := NewPollRequest().
		SetChannel("").
		SetAutoAck(false).
		SetMaxItems(0).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.Error(t, err)
	require.Nil(t, response)

}
func TestQueuesClient_Poll_InvalidRequestByServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queueClient, err := NewQueuesClient(ctx,
		base.WithAddress("localhost", 50000),
		base.WithClientId(uuid.New()),
	)
	require.NoError(t, err)
	pollRequest := NewPollRequest().
		SetChannel(">").
		SetAutoAck(false).
		SetMaxItems(0).
		SetWaitTimeout(1000)
	response, err := queueClient.Poll(ctx, pollRequest)
	require.Error(t, err)
	require.Nil(t, response)

}
