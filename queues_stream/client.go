package queues_stream

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go"
	"github.com/kubemq-io/kubemq-go/common"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueuesStreamClient struct {
	sync.Mutex
	clientCtx  context.Context
	client     *GrpcClient
	upstream   *upstream
	downstream *downstream
}

func NewQueuesStreamClient(ctx context.Context, op ...Option) (*QueuesStreamClient, error) {
	client, err := NewGrpcClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	c := &QueuesStreamClient{
		clientCtx: ctx,
		client:    client,
	}

	return c, nil
}
func (q *QueuesStreamClient) Send(ctx context.Context, messages ...*QueueMessage) (*SendResult, error) {
	q.Lock()
	if q.upstream == nil {
		q.upstream = newUpstream(ctx, q)
	}
	q.Unlock()
	if !q.upstream.isReady() {
		return nil, fmt.Errorf("kubemq grpc client connection lost, can't send messages ")
	}
	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages to send")
	}
	var list []*pb.QueueMessage
	for _, message := range messages {
		list = append(list, message.complete(q.client.GlobalClientId()).QueueMessage)
	}
	req := &pb.QueuesUpstreamRequest{
		RequestID: uuid.New(),
		Messages:  list,
	}
	select {
	case resp := <-q.upstream.send(req):
		if resp.IsError {
			return nil, fmt.Errorf(resp.Error)
		}
		return newSendResult(resp.Results), nil
	case <-ctx.Done():
		q.upstream.cancelTransaction(req.RequestID)
		return nil, ctx.Err()
	}
}

func (q *QueuesStreamClient) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	q.Lock()
	if q.downstream == nil {
		q.downstream = newDownstream(ctx, q)
	}
	q.Unlock()
	if !q.downstream.isReady() {
		return nil, fmt.Errorf("kubemq grpc client connection lost, can't poll messages")
	}
	pollReq, err := q.downstream.poll(ctx, request, q.client.GlobalClientId())
	return pollReq, err
}

func (q *QueuesStreamClient) AckAll(ctx context.Context, request *AckAllRequest) (*AckAllResponse, error) {
	if err := request.validateAndComplete(q.client.GlobalClientId()); err != nil {
		return nil, err
	}
	req := &pb.AckAllQueueMessagesRequest{
		RequestID:       request.requestID,
		ClientID:        request.ClientID,
		Channel:         request.Channel,
		WaitTimeSeconds: request.WaitTimeSeconds,
	}
	pbResp, err := q.client.AckAllQueueMessages(ctx, req)
	if err != nil {
		return nil, err
	}
	resp := &AckAllResponse{
		RequestID:        pbResp.RequestID,
		AffectedMessages: pbResp.AffectedMessages,
		IsError:          pbResp.IsError,
		Error:            pbResp.Error,
	}
	return resp, nil
}

func (q *QueuesStreamClient) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	resp, err := q.client.QueuesInfo(ctx, &pb.QueuesInfoRequest{
		RequestID: uuid.New(),
		QueueName: filter,
	})
	if err != nil {
		return nil, err
	}
	return fromQueuesInfoPb(resp.Info), nil
}

func (q *QueuesStreamClient) Create(ctx context.Context, channel string) error {
	clientId := q.client.GlobalClientId()
	resp, err := q.client.SendRequest(ctx, &pb.Request{
		RequestID:       uuid.New(),
		RequestTypeData: 2,
		ClientID:        clientId,
		Channel:         "kubemq.cluster.internal.requests",
		Metadata:        "create-channel",
		Body:            nil,
		ReplyChannel:    "",
		Timeout:         10000,
		CacheKey:        "",
		CacheTTL:        0,
		Span:            nil,
		Tags: map[string]string{
			"channel_type": "queues",
			"channel":      channel,
			"client_id":    clientId,
		}})

	if err != nil {
		return fmt.Errorf("error sending create channel request: %s", err.Error())
	}

	if resp.Error != "" {
		return fmt.Errorf("error creating channel: %s", resp.Error)
	}

	return nil
}

func (q *QueuesStreamClient) Delete(ctx context.Context, channel string) error {
	clientId := q.client.GlobalClientId()
	resp, err := q.client.SendRequest(ctx, &pb.Request{
		RequestID:       uuid.New(),
		RequestTypeData: 2,
		ClientID:        clientId,
		Channel:         "kubemq.cluster.internal.requests",
		Metadata:        "delete-channel",
		Body:            nil,
		ReplyChannel:    "",
		Timeout:         10000,
		CacheKey:        "",
		CacheTTL:        0,
		Span:            nil,
		Tags: map[string]string{
			"channel_type": "queues",
			"channel":      channel,
			"client_id":    clientId,
		}})

	if err != nil {
		return fmt.Errorf("error sending delete channel request: %s", err.Error())
	}

	if resp.Error != "" {
		return fmt.Errorf("error deleting channel: %s", resp.Error)
	}

	return nil
}

func (q *QueuesStreamClient) List(ctx context.Context, search string) ([]*common.QueuesChannel, error) {
	clientId := q.client.GlobalClientId()
	resp, err := q.client.SendRequest(ctx, &pb.Request{
		RequestID:       uuid.New(),
		RequestTypeData: 2,
		ClientID:        clientId,
		Channel:         "kubemq.cluster.internal.requests",
		Metadata:        "list-channels",
		Body:            nil,
		ReplyChannel:    "",
		Timeout:         10000,
		CacheKey:        "",
		CacheTTL:        0,
		Span:            nil,
		Tags: map[string]string{
			"channel_type": "queues",
			"client_id":    clientId,
			"search":       search,
		}})

	if err != nil {
		return nil, fmt.Errorf("error sending list channel request: %s", err.Error())
	}

	if resp.Error != "" {
		return nil, fmt.Errorf("error list channels: %s", resp.Error)
	}
	channels, err := kubemq.DecodeQueuesChannelList(resp.Body)
	if err != nil {
		return nil, err
	}
	return channels, nil
}

func (q *QueuesStreamClient) Close() error {
	time.Sleep(100 * time.Millisecond)
	if q.upstream != nil {
		q.upstream.close()
	}
	if q.downstream != nil {
		q.downstream.close()
	}
	return q.client.Close()
}
