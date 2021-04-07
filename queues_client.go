package kubemq

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
)

type QueueTransactionMessageRequest struct {
	RequestID         string
	ClientID          string
	Channel           string
	VisibilitySeconds int32
	WaitTimeSeconds   int32
}

func NewQueueTransactionMessageRequest() *QueueTransactionMessageRequest {
	return &QueueTransactionMessageRequest{}
}

// SetId - set receive queue transaction message request id, otherwise new random uuid will be set
func (req *QueueTransactionMessageRequest) SetId(id string) *QueueTransactionMessageRequest {
	req.RequestID = id
	return req
}

// SetClientId - set receive queue transaction message request ClientId - mandatory if default client was not set
func (req *QueueTransactionMessageRequest) SetClientId(clientId string) *QueueTransactionMessageRequest {
	req.ClientID = clientId
	return req
}

// SetChannel - set receive queue transaction message request channel - mandatory if default channel was not set
func (req *QueueTransactionMessageRequest) SetChannel(channel string) *QueueTransactionMessageRequest {
	req.Channel = channel
	return req
}

// SetWaitTimeSeconds - set receive queue transaction message request wait timout for receiving queue message
func (req *QueueTransactionMessageRequest) SetWaitTimeSeconds(wait int) *QueueTransactionMessageRequest {
	req.WaitTimeSeconds = int32(wait)
	return req
}

// SetVisibilitySeconds - set receive queue transaction message visibility seconds for hiding message from other clients during processing
func (req *QueueTransactionMessageRequest) SetVisibilitySeconds(visibility int) *QueueTransactionMessageRequest {
	req.VisibilitySeconds = int32(visibility)
	return req
}

type QueuesClient struct {
	client *Client
}

func NewQueuesClient(ctx context.Context, op ...Option) (*QueuesClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueuesClient{
		client: client,
	}, nil
}

func (q *QueuesClient) Send(ctx context.Context, message *QueueMessage) (*SendQueueMessageResult, error) {
	return q.client.SetQueueMessage(message).Send(ctx)
}

func (q *QueuesClient) Batch(ctx context.Context, messages []*QueueMessage) ([]*SendQueueMessageResult, error) {
	batchRequest := &QueueMessages{
		Messages: messages,
	}
	batchRequest.transport = q.client.transport
	return batchRequest.Send(ctx)
}

func (q *QueuesClient) Pull(ctx context.Context, request *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	request.SetIsPeak(false)
	return request.Send(ctx)
}
func (q *QueuesClient) Peek(ctx context.Context, request *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	request.SetIsPeak(true)
	return request.Send(ctx)
}
func (q *QueuesClient) AckAll(ctx context.Context, request *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
	return request.Send(ctx)
}

func (q *QueuesClient) Transaction(ctx context.Context, request *QueueTransactionMessageRequest) (*QueueTransactionMessageResponse, error) {
	grpcClient, err := q.client.transport.GetGRPCRawClient()
	if err != nil {
		return nil, err
	}
	stream, err := grpcClient.StreamQueueMessage(ctx)
	if err != nil {
		return nil, err
	}
	if request.ClientID == "" {
		request.ClientID = q.client.opts.clientId
	}
	err = stream.Send(&pb.StreamQueueMessagesRequest{
		RequestID:             request.RequestID,
		ClientID:              request.ClientID,
		StreamRequestTypeData: pb.ReceiveMessage,
		Channel:               request.Channel,
		VisibilitySeconds:     request.VisibilitySeconds,
		WaitTimeSeconds:       request.WaitTimeSeconds,
	})
	if err != nil {
		return nil, err
	}

	res, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	if res.IsError {
		return nil, fmt.Errorf("%s", res.Error)
	}
	return &QueueTransactionMessageResponse{
		client:  q,
		stream:  stream,
		Message: &QueueMessage{QueueMessage: res.Message},
	}, nil
}
func (q *QueuesClient) Close() error {
	return q.client.Close()
}

type QueueTransactionMessageResponse struct {
	client  *QueuesClient
	stream  pb.Kubemq_StreamQueueMessageClient
	Message *QueueMessage
}

func (qt *QueueTransactionMessageResponse) transact(req *pb.StreamQueueMessagesRequest) error {
	if qt.stream == nil {
		return fmt.Errorf("no active connection established")
	}

	errCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)
	go func() {
		err := qt.stream.Send(req)
		if err != nil {
			errCh <- err
		} else {
			doneCh <- struct{}{}
		}
	}()
	res, err := qt.stream.Recv()
	if err != nil {
		return err
	}
	if res.IsError {
		return fmt.Errorf("%s", res.Error)
	}

	select {
	case err := <-errCh:
		return err
	case <-doneCh:

	}
	return nil
}

func (qt *QueueTransactionMessageResponse) Ack() error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.AckMessage,
		Channel:               qt.Message.Channel,
		RefSequence:           qt.Message.Attributes.Sequence,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}
func (qt *QueueTransactionMessageResponse) Reject() error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.RejectMessage,
		Channel:               qt.Message.Channel,
		RefSequence:           qt.Message.Attributes.Sequence,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}

func (qt *QueueTransactionMessageResponse) ExtendVisibility(value int) error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.ModifyVisibility,
		Channel:               qt.Message.Channel,
		VisibilitySeconds:     int32(value),
		RefSequence:           qt.Message.Attributes.Sequence,
	})
	if err != nil {
		return err
	}
	return nil
}

func (qt *QueueTransactionMessageResponse) Resend(channel string) error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.ResendMessage,
		Channel:               channel,
		RefSequence:           qt.Message.Attributes.Sequence,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}
func (qt *QueueTransactionMessageResponse) ResendNewMessage(msg *QueueMessage) error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.SendModifiedMessage,
		Channel:               qt.Message.Channel,
		RefSequence:           qt.Message.Attributes.Sequence,
		ModifiedMessage:       msg.QueueMessage,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}
