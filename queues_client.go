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
func (req *QueueTransactionMessageRequest) Complete(opts *Options) *QueueTransactionMessageRequest {
	if req.ClientID == "" {
		req.ClientID = opts.clientId
	}
	return req
}
func (req *QueueTransactionMessageRequest) Validate() error {
	if req.Channel == "" {
		return fmt.Errorf("request must have a channel")
	}
	if req.ClientID == "" {
		return fmt.Errorf("request must have a clientId")
	}

	if req.WaitTimeSeconds <= 0 {
		return fmt.Errorf("request must have a wait time seconds >0")
	}

	if req.VisibilitySeconds <= 0 {
		return fmt.Errorf("request must have a visibility seconds >0")
	}

	return nil
}

type QueuesClient struct {
	client *Client
}

func NewQueuesStreamClient(ctx context.Context, op ...Option) (*QueuesClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueuesClient{
		client: client,
	}, nil
}

func (q *QueuesClient) Send(ctx context.Context, message *QueueMessage) (*SendQueueMessageResult, error) {
	message.transport = q.client.transport
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
	request.transport = q.client.transport
	request.SetIsPeak(false)
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	return request.Send(ctx)
}

func (q *QueuesClient) Subscribe(ctx context.Context, request *ReceiveQueueMessagesRequest, onReceive func(response *ReceiveQueueMessagesResponse, err error)) (chan struct{}, error) {
	request.transport = q.client.transport
	if onReceive == nil {
		return nil, fmt.Errorf("queue subscription callback function is required")
	}
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	done := make(chan struct{}, 1)
	if request.ClientID == "" {
		request.ClientID = q.client.opts.clientId
	}
	request.transport = q.client.transport
	request.SetIsPeak(false)
	go func() {
		for {
			resp, err := request.Send(ctx)
			if err != nil {
				onReceive(nil, err)
			} else {
				if resp.IsError {
					onReceive(nil, fmt.Errorf(resp.Error))
				} else {
					if len(resp.Messages) > 0 {
						onReceive(resp, nil)
					}
				}
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:

			}
		}
	}()
	return done, nil
}

func (q *QueuesClient) Peek(ctx context.Context, request *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	request.transport = q.client.transport
	request.SetIsPeak(true)
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	return request.Send(ctx)
}
func (q *QueuesClient) AckAll(ctx context.Context, request *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
	request.transport = q.client.transport
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	return request.Send(ctx)
}

func (q *QueuesClient) TransactionStream(ctx context.Context, request *QueueTransactionMessageRequest, onReceive func(response *QueueTransactionMessageResponse, err error)) (chan struct{}, error) {
	if onReceive == nil {
		return nil, fmt.Errorf("queue transaction stream callback function is required")
	}
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	done := make(chan struct{}, 1)
	go func() {
		for {
			resp, err := q.Transaction(ctx, request)
			if err != nil {
				onReceive(nil, err)
			} else {
				onReceive(resp, nil)
			}
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			default:

			}
		}
	}()
	return done, nil
}

func (q *QueuesClient) Transaction(ctx context.Context, request *QueueTransactionMessageRequest) (*QueueTransactionMessageResponse, error) {
	grpcClient, err := q.client.transport.GetGRPCRawClient()
	if err != nil {
		return nil, err
	}
	if err := request.Complete(q.client.opts).Validate(); err != nil {
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
		StreamRequestTypeData: pb.StreamRequestType_ReceiveMessage,
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
func (q *QueuesClient) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	return q.client.transport.QueuesInfo(ctx, filter)
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
		StreamRequestTypeData: pb.StreamRequestType_AckMessage,
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
		StreamRequestTypeData: pb.StreamRequestType_RejectMessage,
		Channel:               qt.Message.Channel,
		RefSequence:           qt.Message.Attributes.Sequence,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}

func (qt *QueueTransactionMessageResponse) ExtendVisibilitySeconds(value int) error {
	err := qt.transact(&pb.StreamQueueMessagesRequest{
		RequestID:             qt.Message.MessageID,
		ClientID:              qt.Message.ClientID,
		StreamRequestTypeData: pb.StreamRequestType_ModifyVisibility,
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
		StreamRequestTypeData: pb.StreamRequestType_ResendMessage,
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
		StreamRequestTypeData: pb.StreamRequestType_SendModifiedMessage,
		Channel:               qt.Message.Channel,
		RefSequence:           qt.Message.Attributes.Sequence,
		ModifiedMessage:       msg.QueueMessage,
	})
	if err != nil {
		return err
	}
	return qt.stream.CloseSend()
}
