package kubemq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/kubemq-io/kubemq-go/pb"
)

type QueueMessage struct {
	Id         string
	ClientId   string
	Channel    string
	Metadata   string
	Body       []byte
	Tags       map[string]string
	Attributes *QueueMessageAttributes
	Policy     *QueueMessagePolicy
	transport  Transport
	trace      *Trace
}

// SetId - set queue message id, otherwise new random uuid will be set
func (qm *QueueMessage) SetId(id string) *QueueMessage {
	qm.Id = id
	return qm

}

// SetClientId - set queue message ClientId - mandatory if default client was not set
func (qm *QueueMessage) SetClientId(clientId string) *QueueMessage {
	qm.ClientId = clientId
	return qm
}

// SetChannel - set queue message channel - mandatory if default channel was not set
func (qm *QueueMessage) SetChannel(channel string) *QueueMessage {
	qm.Channel = channel
	return qm
}

// SetMetadata - set queue message metadata - mandatory if body field is empty
func (qm *QueueMessage) SetMetadata(metadata string) *QueueMessage {
	qm.Metadata = metadata
	return qm
}

// SetBody - set queue message body - mandatory if metadata field is empty
func (qm *QueueMessage) SetBody(body []byte) *QueueMessage {
	qm.Body = body
	return qm
}

// AddTag - add key value tags to query message
func (qm *QueueMessage) AddTag(key,value string) *QueueMessage {
	qm.Tags[key] = value
	return qm
}


// SetPolicyExpirationSeconds - set queue message expiration seconds, 0 never expires
func (qm *QueueMessage) SetPolicyExpirationSeconds(sec int) *QueueMessage {
	qm.Policy.ExpirationSeconds = int32(sec)
	return qm
}

// SetPolicySeconds - set queue message delivery delay in seconds, 0 , immediate delivery
func (qm *QueueMessage) SetPolicySeconds(sec int) *QueueMessage {
	qm.Policy.DelaySeconds = int32(sec)
	return qm
}

// SetPolicyMaxReceiveCount - set max delivery attempts before message will discard or re-route to a new queue
func (qm *QueueMessage) SetPolicyMaxReceiveCount(max int) *QueueMessage {
	qm.Policy.MaxReceiveCount = int32(max)
	return qm
}

// SetPolicyMaxReceiveQueue - set queue name to be routed once MaxReceiveCount is triggered, empty will discard the message
func (qm *QueueMessage) SetPolicyMaxReceiveQueue(channel string) *QueueMessage {
	qm.Policy.MaxReceiveQueue = channel
	return qm
}

// Send - sending queue message request , waiting for response or timeout
func (qm *QueueMessage) Send(ctx context.Context) (*SendQueueMessageResult, error) {
	return qm.transport.SendQueueMessage(ctx, qm)
}

// AddTrace - add tracing support to queue message
func (qm *QueueMessage) AddTrace(name string) *Trace {
	qm.trace = CreateTrace(name)
	return qm.trace
}

type QueueMessages struct {
	Messages  []*QueueMessage
	transport Transport
}

// Add - adding new queue message to array of messages
func (qma *QueueMessages) Add(msg *QueueMessage) *QueueMessages {
	qma.Messages = append(qma.Messages, msg)
	return qma
}

// Send - sending queue messages array request , waiting for response or timeout
func (qma *QueueMessages) Send(ctx context.Context) ([]*SendQueueMessageResult, error) {
	if qma.transport == nil {
		return nil, ErrNoTransportDefined
	}
	return qma.transport.SendQueueMessages(ctx, qma.Messages)
}

type QueueMessageAttributes struct {
	Timestamp         int64
	Sequence          uint64
	MD5OfBody         string
	ReceiveCount      int32
	ReRouted          bool
	ReRoutedFromQueue string
	ExpirationAt      int64
	DelayedTo         int64
}

type QueueMessagePolicy struct {
	ExpirationSeconds int32
	DelaySeconds      int32
	MaxReceiveCount   int32
	MaxReceiveQueue   string
}

type SendQueueMessageResult struct {
	MessageID    string
	SentAt       int64
	ExpirationAt int64
	DelayedTo    int64
	IsError      bool
	Error        string
}

type ReceiveQueueMessagesRequest struct {
	RequestID           string
	ClientID            string
	Channel             string
	MaxNumberOfMessages int32
	WaitTimeSeconds     int32
	IsPeak              bool
	transport           Transport
	trace               *Trace
}

// SetId - set receive queue message request id, otherwise new random uuid will be set
func (req *ReceiveQueueMessagesRequest) SetId(id string) *ReceiveQueueMessagesRequest {
	req.RequestID = id
	return req
}

// SetClientId - set receive queue message request ClientId - mandatory if default client was not set
func (req *ReceiveQueueMessagesRequest) SetClientId(clientId string) *ReceiveQueueMessagesRequest {
	req.ClientID = clientId
	return req
}

// SetChannel - set receive queue message request channel - mandatory if default channel was not set
func (req *ReceiveQueueMessagesRequest) SetChannel(channel string) *ReceiveQueueMessagesRequest {
	req.Channel = channel
	return req
}

// SetMaxNumberOfMessages - set receive queue message request max number of messages to receive in single call
func (req *ReceiveQueueMessagesRequest) SetMaxNumberOfMessages(max int) *ReceiveQueueMessagesRequest {
	req.MaxNumberOfMessages = int32(max)
	return req
}

// SetWaitTimeSeconds - set receive queue message request wait timout for receiving all requested messages
func (req *ReceiveQueueMessagesRequest) SetWaitTimeSeconds(wait int) *ReceiveQueueMessagesRequest {
	req.WaitTimeSeconds = int32(wait)
	return req
}

// SetIsPeak - set receive queue message request type, true - peaking at the queue and not actual dequeue , false - dequeue the queue
func (req *ReceiveQueueMessagesRequest) SetIsPeak(value bool) *ReceiveQueueMessagesRequest {
	req.IsPeak = value
	return req
}

// AddTrace - add tracing support to receive queue message request
func (req *ReceiveQueueMessagesRequest) AddTrace(name string) *Trace {
	req.trace = CreateTrace(name)
	return req.trace
}

// Send - sending receive queue messages request , waiting for response or timeout
func (req *ReceiveQueueMessagesRequest) Send(ctx context.Context) (*ReceiveQueueMessagesResponse, error) {
	if req.transport == nil {
		return nil, ErrNoTransportDefined
	}
	return req.transport.ReceiveQueueMessages(ctx, req)
}

type ReceiveQueueMessagesResponse struct {
	RequestID        string
	Messages         []*QueueMessage
	MessagesReceived int32
	MessagesExpired  int32
	IsPeak           bool
	IsError          bool
	Error            string
}

type AckAllQueueMessagesRequest struct {
	RequestID       string
	ClientID        string
	Channel         string
	WaitTimeSeconds int32
	transport       Transport
	trace           *Trace
}

// SetId - set ack all queue message request id, otherwise new random uuid will be set
func (req *AckAllQueueMessagesRequest) SetId(id string) *AckAllQueueMessagesRequest {
	req.RequestID = id
	return req
}

// SetClientId - set ack all queue message request ClientId - mandatory if default client was not set
func (req *AckAllQueueMessagesRequest) SetClientId(clientId string) *AckAllQueueMessagesRequest {
	req.ClientID = clientId
	return req
}

// SetChannel - set ack all queue message request channel - mandatory if default channel was not set
func (req *AckAllQueueMessagesRequest) SetChannel(channel string) *AckAllQueueMessagesRequest {
	req.Channel = channel
	return req
}

// SetWaitTimeSeconds - set ack all queue message request wait timout
func (req *AckAllQueueMessagesRequest) SetWaitTimeSeconds(wait int) *AckAllQueueMessagesRequest {
	req.WaitTimeSeconds = int32(wait)
	return req
}

// AddTrace - add tracing support to ack all receive queue message request
func (req *AckAllQueueMessagesRequest) AddTrace(name string) *Trace {
	req.trace = CreateTrace(name)
	return req.trace
}

// Send - sending receive queue messages request , waiting for response or timeout
func (req *AckAllQueueMessagesRequest) Send(ctx context.Context) (*AckAllQueueMessagesResponse, error) {
	if req.transport == nil {
		return nil, ErrNoTransportDefined
	}
	return req.transport.AckAllQueueMessages(ctx, req)
}

type AckAllQueueMessagesResponse struct {
	RequestID        string
	AffectedMessages uint64
	IsError          bool
	Error            string
}

type StreamQueueMessage struct {
	RequestID         string
	ClientID          string
	Channel           string
	visibilitySeconds int32
	waitTimeSeconds   int32
	refSequence       uint64
	reqCh             chan *pb.StreamQueueMessagesRequest
	resCh             chan *pb.StreamQueueMessagesResponse
	errCh             chan error
	doneCh            chan bool
	msg               *QueueMessage
	transport         Transport
	trace             *Trace
	ctx               context.Context
	cancel            context.CancelFunc
	isCompleted       bool
	mu                sync.Mutex
}

// SetId - set stream queue message request id, otherwise new random uuid will be set
func (req *StreamQueueMessage) SetId(id string) *StreamQueueMessage {
	req.RequestID = id
	return req
}

// SetClientId - set stream queue message request ClientId - mandatory if default client was not set
func (req *StreamQueueMessage) SetClientId(clientId string) *StreamQueueMessage {
	req.ClientID = clientId
	return req
}

// SetChannel - set stream queue message request channel - mandatory if default channel was not set
func (req *StreamQueueMessage) SetChannel(channel string) *StreamQueueMessage {
	req.Channel = channel
	return req
}

// AddTrace - add tracing support to stream receive queue message request
func (req *StreamQueueMessage) AddTrace(name string) *Trace {
	req.trace = CreateTrace(name)
	return req.trace
}

// Receive - receive queue messages request , waiting for response or timeout
func (req *StreamQueueMessage) Receive(ctx context.Context, visibility, wait int32) (*QueueMessage, error) {
	req.mu.Lock()
	defer req.mu.Unlock()
	if req.transport == nil {
		return nil, ErrNoTransportDefined
	}
	if req.msg != nil {
		return nil, errors.New("active queue message wait for ack/reject")
	}
	req.reqCh = make(chan *pb.StreamQueueMessagesRequest, 1)
	req.resCh = make(chan *pb.StreamQueueMessagesResponse, 1)
	req.errCh = make(chan error, 2)
	req.doneCh = make(chan bool, 2)
	req.ctx, req.cancel = context.WithTimeout(ctx, time.Duration(wait+1)*time.Second)
	go req.transport.StreamQueueMessage(req.ctx, req.reqCh, req.resCh, req.errCh, req.doneCh)

	go func() {
		for {
			select {
			case <-req.doneCh:
				req.isCompleted = true
				req.msg = nil
				req.cancel()
				return
			case <-req.ctx.Done():
				return
			}
		}

	}()

	getRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.ReceiveMessage,
		Channel:               req.Channel,
		VisibilitySeconds:     visibility,
		WaitTimeSeconds:       wait,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}

	req.reqCh <- getRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return nil, errors.New(getResponse.Error)
		}
		if getResponse.Message == nil {
			return nil, errors.New("no new queue message available")
		}
		resMsg := getResponse.Message
		req.msg = &QueueMessage{
			Id:       resMsg.MessageID,
			ClientId: resMsg.ClientID,
			Channel:  resMsg.Channel,
			Metadata: resMsg.Metadata,
			Body:     resMsg.Body,
			Tags:     resMsg.Tags,
			Attributes: &QueueMessageAttributes{
				Timestamp:         resMsg.Attributes.Timestamp,
				Sequence:          resMsg.Attributes.Sequence,
				MD5OfBody:         resMsg.Attributes.MD5OfBody,
				ReceiveCount:      resMsg.Attributes.ReceiveCount,
				ReRouted:          resMsg.Attributes.ReRouted,
				ReRoutedFromQueue: resMsg.Attributes.ReRoutedFromQueue,
				ExpirationAt:      resMsg.Attributes.ExpirationAt,
				DelayedTo:         resMsg.Attributes.DelayedTo,
			},
			Policy: &QueueMessagePolicy{
				ExpirationSeconds: resMsg.Policy.ExpirationSeconds,
				DelaySeconds:      resMsg.Policy.DelaySeconds,
				MaxReceiveCount:   resMsg.Policy.MaxReceiveCount,
				MaxReceiveQueue:   resMsg.Policy.MaxReceiveQueue,
			},
		}

		return req.msg, nil
	case err := <-req.errCh:
		return nil, err
	case <-req.ctx.Done():
		return nil, nil
	}

}

// Ack - ack the received queue messages waiting for response or timeout
func (req *StreamQueueMessage) Ack() error {
	if req.msg == nil {
		return errors.New("no active message to ack, call Receive first")
	}

	ackRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.AckMessage,
		Channel:               req.Channel,
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           req.msg.Attributes.Sequence,
		ModifiedMessage:       nil,
	}
	req.reqCh <- ackRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return errors.New(getResponse.Error)
		}
	case err := <-req.errCh:
		return err
	case <-req.ctx.Done():
		return nil
	}
	return nil
}

// Reject - reject the received queue messages waiting for response or timeout
func (req *StreamQueueMessage) Reject() error {
	if req.msg == nil {
		return errors.New("no active message to reject, call Receive first")
	}

	rejRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.RejectMessage,
		Channel:               req.Channel,
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           req.msg.Attributes.Sequence,
		ModifiedMessage:       nil,
	}
	req.reqCh <- rejRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return errors.New(getResponse.Error)
		}
	case err := <-req.errCh:
		return err
	case <-req.ctx.Done():
		return nil
	}
	return nil
}

// ExtendVisibility - extend the visibility time for the current receive message
func (req *StreamQueueMessage) ExtendVisibility(value int32) error {
	if req.msg == nil {
		return errors.New("no active message to extend visibility, call Receive first")
	}

	extRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.ModifyVisibility,
		Channel:               req.Channel,
		VisibilitySeconds:     value,
		WaitTimeSeconds:       0,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
	req.reqCh <- extRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return errors.New(getResponse.Error)
		}
	case err := <-req.errCh:
		return err
	case <-req.ctx.Done():
		return nil
	}
	return nil
}

// Resend - resend the current received message to a new channel and ack the current message
func (req *StreamQueueMessage) Resend(channel string) error {
	if req.msg == nil {
		return errors.New("no active message to resend, call Receive first")
	}

	extRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.ResendMessage,
		Channel:               channel,
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           0,
		ModifiedMessage:       nil,
	}
	req.reqCh <- extRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return errors.New(getResponse.Error)
		}
	case err := <-req.errCh:
		return err
	case <-req.ctx.Done():
		return nil
	}
	return nil
}

// ResendWithNewMessage - resend the current received message to a new channel
func (req *StreamQueueMessage) ResendWithNewMessage(msg *QueueMessage) error {
	if req.msg == nil {
		return errors.New("no active message to resend, call Receive first")
	}
	extRequest := &pb.StreamQueueMessagesRequest{
		RequestID:             req.RequestID,
		ClientID:              req.ClientID,
		StreamRequestTypeData: pb.SendModifiedMessage,
		Channel:               "",
		VisibilitySeconds:     0,
		WaitTimeSeconds:       0,
		RefSequence:           0,
		ModifiedMessage: &pb.QueueMessage{
			MessageID:  msg.Id,
			ClientID:   msg.ClientId,
			Channel:    msg.Channel,
			Metadata:   msg.Metadata,
			Body:       msg.Body,
			Tags:       msg.Tags,
			Attributes: &pb.QueueMessageAttributes{},
			Policy: &pb.QueueMessagePolicy{
				ExpirationSeconds: msg.Policy.ExpirationSeconds,
				DelaySeconds:      msg.Policy.DelaySeconds,
				MaxReceiveCount:   msg.Policy.MaxReceiveCount,
				MaxReceiveQueue:   msg.Policy.MaxReceiveQueue,
			},
		},
	}
	req.reqCh <- extRequest
	select {
	case getResponse := <-req.resCh:
		if getResponse.IsError {
			return errors.New(getResponse.Error)
		}
	case err := <-req.errCh:
		return err
	case <-req.ctx.Done():
		return nil
	}
	return nil
}
