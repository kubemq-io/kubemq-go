package queues

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/clients"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueuesClient struct {
	clientCtx  context.Context
	client     *clients.GrpcClient
	upstream   *upstream
	downstream *downstream
}

func NewQueuesClient(ctx context.Context, op ...clients.Option) (*QueuesClient, error) {
	client, err := clients.NewGrpcClient(ctx, op...)
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
	pollReq, err := q.downstream.poll(ctx, request, q.client.GlobalClientId())
	return pollReq, err
}

func (q *QueuesClient) AckAll(ctx context.Context, request *AckAllRequest) (*AckAllResponse, error) {
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

func (q *QueuesClient) Close() error {
	q.upstream.close()
	q.downstream.close()
	return q.client.Close()
}
