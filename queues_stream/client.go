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

// QueuesStreamClient is a client for streaming queues operations.
// It manages the connection to the Kubemq server and provides methods for interacting with queues.
type QueuesStreamClient struct {
	sync.Mutex
	clientCtx  context.Context
	client     *GrpcClient
	upstream   *upstream
	downstream *downstream
}

// NewQueuesStreamClient is a function that creates a new QueuesStreamClient instance.
// It takes a context and an optional list of options.
// It returns a QueuesStreamClient and an error.
// The function creates a new GrpcClient using the provided context and options.
// If the creation of the GrpcClient fails, an error is returned.
// Otherwise, a new QueuesStreamClient is created with the client context and client.
// The QueuesStreamClient is then returned along with a nil error.
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

// Send sends one or more QueueMessages to the QueuesStreamClient.
// It acquires a lock to ensure thread safety.
// If there is no upstream connection, a new upstream connection is created.
// If the upstream connection is not ready, an error is returned.
// If there are no messages to send, an error is returned.
// The messages are converted to a list of pb.QueueMessage structs.
// A new QueuesUpstreamRequest is created with a unique RequestID and the list of messages.
// The request is sent to the upstream server using the send method of the upstream connection.
// The response is received through a select statement, handling both the response and the cancellation of the request.
// If the response indicates an error, an error is returned.
// Otherwise, a new SendResult is created from the response and returned along with no error.
// If the context is canceled before receiving the response, the transaction is canceled and an error is returned.
// If any other error occurs during the execution, it is returned.
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

// Poll retrieves messages from the QueuesStreamClient.
// It checks if the downstream connection is ready and then calls downstream.poll()
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

// AckAll sends an acknowledgment for all messages in the specified channel.
// It validates the request, creates a new AckAllQueueMessagesRequest,
// and sends it to the client's AckAllQueueMessages method. If successful,
// it creates a new AckAllResponse with the response data and returns it.
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

// QueuesInfo returns information about queues based on the provided filter.
// It sends a request to the gRPC client to retrieve QueuesInfoResponse.
// The response is then transformed into a QueuesInfo struct and returned.
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

// Create sends a create channel request to the Kubemq server.
// It creates a new channel of type "queues" with the specified channel name.
// It returns an error if the request fails or if there is an error creating the channel.
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

// Delete deletes a channel in the QueuesStreamClient. It sends a request to the
// server to delete the specified channel. If the request encounters an error
// while sending or if the response contains an error message, an error is returned.
//
// Parameters:
// - ctx: the context.Context for the request
// - channel: the name of the channel to delete
//
// Returns:
// - error: an error if the delete channel request fails, otherwise nil
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

// List returns a list of QueuesChannels based on the given search string.
// It sends a request to the Kubemq server to retrieve the list of channels.
// The search string is used to filter the channels.
// It returns the list of QueuesChannels and any error encountered.
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

// Close closes the QueuesStreamClient by closing the upstream and downstream connections
// and then closing the underlying GrpcClient connection.
// It also sleeps for 100 milliseconds before closing the connections.
// Returns an error if any of the close operations encounter an error, or if closing
// the underlying GrpcClient encounters an error.
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
