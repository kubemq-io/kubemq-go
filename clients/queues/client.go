package queues

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/clients/base"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueuesClient struct {
	clientCtx  context.Context
	client     *base.GrpcClient
	upstream   *upstream
	downstream *downstream
}

func NewQueuesClient(ctx context.Context, op ...base.Option) (*QueuesClient, error) {
	client, err := base.NewGrpcClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueuesClient{
		clientCtx:  ctx,
		client:     client,
		upstream:   newUpstream(ctx, client.KubemqClient),
		downstream: newDownstream(ctx, client.KubemqClient),
	}, nil
}
func (q *QueuesClient) Send(ctx context.Context, messages ...*QueueMessage) (*SendResult, error) {
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

func (q *QueuesClient) Poll(ctx context.Context, request *PollRequest) (*PollResponse, error) {
	req, err := request.validateAndComplete(q.client.GlobalClientId())
	if err != nil {
		return nil, err
	}
	pollReq, err := q.downstream.poll(ctx, req)
	return pollReq, err
}

func (q *QueuesClient) Subscribe(ctx context.Context, request *SubscribeRequest, cb func(response *SubscribeResponse, err error)) error {
	if cb == nil {
		return fmt.Errorf("subscribe callback cannot be nil")
	}
	return nil
}

//func (q *QueuesClient) AckAll(ctx context.Context, request *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
//	request.transport = q.client.transport
//	if err := request.Complete(q.client.opts).Validate(); err != nil {
//		return nil, err
//	}
//	return request.Send(ctx)
//}

func (q *QueuesClient) Close() error {
	q.upstream.close()
	q.downstream.close()
	return q.client.Close()
}
