package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
	pb "github.com/kubemq-io/protobuf/go"
)

// QueueTransactionMessageRequest represents a request to enqueue a transaction message on a queue.
// It contains the following fields:
// - RequestID: The ID of the request.
// - ClientID: The ID of the client.
// - Channel: The channel to enqueue the message on.
// - VisibilitySeconds: The number of seconds for which the message will be hidden from other clients.
// - WaitTimeSeconds: The number of seconds to wait for a message to be received from the queue.
//
// It has the following methods:
// - SetId: Set the request ID.
// - SetClientId: Set the client ID.
// - SetChannel: Set the channel.
// - SetWaitTimeSeconds: Set the wait time in seconds.
// - SetVisibilitySeconds: Set the visibility time in seconds.
// - Complete: Complete the request using the specified options.
// - Validate: Validate that the request is valid.
//
// Usage example:
//
//	req := NewQueueTransactionMessageRequest().
//		SetId("123").
//		SetClientId("456").
//		SetChannel("channel").
//		SetWaitTimeSeconds(60).
//		SetVisibilitySeconds(30)
//
//	err := req.Validate()
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	client := NewClient()
//	resp, err := client.Transaction(ctx, req)
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	fmt.Println(resp)
type QueueTransactionMessageRequest struct {
	RequestID         string
	ClientID          string
	Channel           string
	VisibilitySeconds int32
	WaitTimeSeconds   int32
}

// NewQueueTransactionMessageRequest - create a new instance of QueueTransactionMessageRequest
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

// SetWaitTimeSeconds - set receive queue transaction message request wait timout for receiving queue message.
func (req *QueueTransactionMessageRequest) SetWaitTimeSeconds(wait int) *QueueTransactionMessageRequest {
	req.WaitTimeSeconds = int32(wait)
	return req
}

// SetVisibilitySeconds - set receive queue transaction message visibility seconds
// for hiding message from other clients during processing.
// It takes an integer argument 'visibility' as the number of seconds.
// It sets the visibility seconds of the request to the given value.
// The updated QueueTransactionMessageRequest is returned.
func (req *QueueTransactionMessageRequest) SetVisibilitySeconds(visibility int) *QueueTransactionMessageRequest {
	req.VisibilitySeconds = int32(visibility)
	return req
}

// Complete sets the ClientID field of the QueueTransactionMessageRequest to the value
// of opts.clientId if the ClientID field is empty. It returns the modified
// QueueTransactionMessageRequest.
func (req *QueueTransactionMessageRequest) Complete(opts *Options) *QueueTransactionMessageRequest {
	if req.ClientID == "" {
		req.ClientID = opts.clientId
	}
	return req
}

// Validate checks if the QueueTransactionMessageRequest is valid.
// It returns an error if any of the mandatory fields are empty
// or if any of the numeric fields are less than or equal to zero.
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

// QueuesClient is a client for managing queues in a messaging system.
type QueuesClient struct {
	client *Client
}

// NewQueuesClient - create a new client for interacting with KubeMQ Queues
// Parameters:
//   - ctx: the context.Context used for the client
//   - op...: options to configure the client
//
// Returns:
//   - *QueuesClient: the created Queues client instance
//   - error: any error that occurred during the creation of the client
func NewQueuesClient(ctx context.Context, op ...Option) (*QueuesClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueuesClient{
		client: client,
	}, nil
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

// Send sends a queue message using the specified context and message.
// The transport field of the message is set to the transport of the QueuesClient.
// Returns the result of the SendQueueMessage method of the client, with the provided context.
//
// ctx - The context to use for the send operation.
// message - The queue message to send.
func (q *QueuesClient) Send(ctx context.Context, message *QueueMessage) (*SendQueueMessageResult, error) {
	message.transport = q.client.transport
	return q.client.SetQueueMessage(message).Send(ctx)
}

// Batch sends a batch of queue messages in a single request.
// It creates a new QueueMessages request with the given messages and sets the transport to the client's transport.
// It then calls the Send method of the QueueMessages request with the provided context and returns the result.
func (q *QueuesClient) Batch(ctx context.Context, messages []*QueueMessage) ([]*SendQueueMessageResult, error) {
	batchRequest := &QueueMessages{
		Messages: messages,
	}
	batchRequest.transport = q.client.transport
	return batchRequest.Send(ctx)
}

// Pull - Pulls messages from a queue using the provided request parameters.
// The request parameters include the channel, maximum number of messages to receive,
// wait time, and whether to peak or dequeue the queue.
// The method sets the transport field of the request to the QueuesClient's transport,
// sets the IsPeak field of the request to false, completes and validates the request,
// and sends the request using the provided context.
// It returns the response received from the request or an error if any.
func (q *QueuesClient) Pull(ctx context.Context, request *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	request.transport = q.client.transport
	request.SetIsPeak(false)
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return nil, err
	}
	return request.Send(ctx)
}

// Subscribe receives queue messages and executes the provided callback function
// onReceive when messages are received. It takes a context.Context object ctx,
// a *ReceiveQueueMessagesRequest object request, and a callback function
// onReceive that accepts a *ReceiveQueueMessagesResponse object and an error
// object. It returns a channel of type struct{} and an error. The channel can
// be used to signal the completion of the subscription and stop the subscription
// process. The function spawns a goroutine that continuously sends requests to
// receive queue messages until the subscription is stopped or the context is
// canceled. When a response is received, the onReceive callback is executed with
// the response and any error as arguments. If an error occurs during the request
// or response processing, the onReceive callback is executed with a nil response
// and the corresponding error.
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

// Peek - retrieve messages from the queue without removing them, using the provided ReceiveQueueMessagesRequest.
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

// TransactionStream - opens a transaction stream for receiving queue transaction messages.
// The provided `onReceive` callback function will be called for each received
// queue transaction message in the stream. The function should have the following signature:
//
//	func(response *QueueTransactionMessageResponse, err error)
//
// The `response` parameter will contain the received queue transaction message,
// or nil if an error occurred. The `err` parameter will contain any error that occurred
// during the receiving process. If the `onReceive` callback function is nil,
// an error will be returned. The `request` parameter represents the queue transaction
// message request. The request must be completed and validated before calling this method.
// The function returns a `done` channel that can be used to stop the transaction stream,
// and an error if one occurred.
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

// Transaction - allows to receive a single message from a queue using transactional processing.
// The method sends a request to the server through a gRPC stream.
// If the gRPC raw client fails to connect, an error is returned.
// If the request fails to complete or validate, an error is returned.
// If the gRPC client fails to stream the queue message, an error is returned.
// If the request has an empty ClientID, it is assigned the default client ID from the client options.
// The request is sent to the gRPC stream using a StreamQueueMessagesRequest.
// If an error occurs while sending the request, an error is returned.
// The response is received from the gRPC stream using a ReceiveQueueMessagesResponse.
// If an error occurs while receiving the response, an error is returned.
// If the response has an error, an error is returned.
// If the response message is not in error, a QueueTransactionMessageResponse is returned,
// including the client, stream, and the QueueMessage.
// The method returns a pointer to the QueueTransactionMessageResponse and an error.
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

// Create - creates a new channel with the specified name in the 'queues' category.
// The channelType is set to "queues".
// It sends a create channel request to the KubeMQ server using the provided client,
// clientId, channel, and channelType.
// It returns an error if the request fails or if an error occurs during channel creation.
// Example:
//
//	err := queuesClient.Create(ctx, "queues.A")
//	if err != nil {
//	   log.Fatal(err)
//	}
//
// It is recommended to defer the closing of the queues client using `defer queuesClient.Close()`
func (q *QueuesClient) Create(ctx context.Context, channel string) error {
	return CreateChannel(ctx, q.client, q.client.opts.clientId, channel, "queues")
}

// Delete deletes a channel by sending a delete channel request to the KubeMQ server.
// It takes a context, a client, a client ID, a channel, and a channel type as parameters.
// The channel is the name of the channel to be deleted, and the channel type is the type of the channel.
// It returns an error if there was a problem sending the delete channel request or if there was an error deleting the channel.
// Example usage:
//
//	if err := queuesClient.Delete(ctx, "queues.A"); err != nil {
//	    log.Fatal(err)
//	}
func (q *QueuesClient) Delete(ctx context.Context, channel string) error {
	return DeleteChannel(ctx, q.client, q.client.opts.clientId, channel, "queues")
}

func (q *QueuesClient) List(ctx context.Context, search string) ([]*common.QueuesChannel, error) {
	return ListQueuesChannels(ctx, q.client, q.client.opts.clientId, search)
}

func (q *QueuesClient) Close() error {
	return q.client.Close()
}

// QueueTransactionMessageResponse represents the response returned from `Transaction` method of QueuesClient.
// It contains the client instance, the stream used for communication, and the Message received.
// It can be used to interact with the response of a transaction operation on a queue message.
type QueueTransactionMessageResponse struct {
	client  *QueuesClient
	stream  pb.Kubemq_StreamQueueMessageClient
	Message *QueueMessage
}

// transact sends a stream queue message request and waits for the response.
// It checks if there is an active connection established, then sends the request
// asynchronously to the stream. If an error occurs during sending the request,
// it returns the error. If the request is sent successfully, it waits for the response.
// If the response indicates an error, it returns an error with the error message from the response.
// Otherwise, it returns nil to indicate success.
// In case of an error received during sending the request, it returns the error immediately.
// If the request is successfully sent, it waits for either an error or a success response.
// If an error response is received, it returns the error.
// If a success response is received, it returns nil.
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

// Ack sends an acknowledgment for the received message by sending the ACK request to the server.
// It returns an error if there is an issue with sending the ACK request or closing the stream connection.
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

// Reject - rejects the queue transaction message by sending a StreamQueueMessagesRequest
// with StreamRequestType_RejectMessage to the Kubemq server. If an error occurs during
// the transaction, it will be returned. Otherwise, it will close the stream connection
// by calling CloseSend() on the QueueTransactionMessageResponse stream.
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

// ResendNewMessage - resends a modified message with a new QueueMessage to the same channel as the original message.
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
