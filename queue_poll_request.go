package kubemq

import (
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"
)

type QueuePollRequest struct {
	RequestID   string
	ClientID    string
	Channel     string
	WaitTimeout int32
	MaxItems    int32
	AutoAck     bool
}

func NewQueuePollRequest() *QueuePollRequest {
	return &QueuePollRequest{
		RequestID: nuid.New().Next(),
	}
}

func (q *QueuePollRequest) Validate() error {
	if q.Channel == "" {
		return fmt.Errorf("invalid request, channel cannot be empty")
	}
	if q.ClientID == "" {
		return fmt.Errorf("invalid request, clientId cannot be empty")
	}
	if q.MaxItems < 0 {
		return fmt.Errorf("invalid request, max items cannot be negative")
	}
	if q.WaitTimeout < 0 {
		return fmt.Errorf("invalid request, wait timeout cannot be negative")
	}
	return nil
}

func (q *QueuePollRequest) SetClientID(clientID string) *QueuePollRequest {
	q.ClientID = clientID
	return q
}

func (q *QueuePollRequest) SetChannel(channel string) *QueuePollRequest {
	q.Channel = channel
	return q
}

func (q *QueuePollRequest) SetWaitTimeSeconds(waitTimeSeconds int32) *QueuePollRequest {
	q.WaitTimeout = waitTimeSeconds
	return q
}

func (q *QueuePollRequest) SetMaxItems(maxItems int32) *QueuePollRequest {
	q.MaxItems = maxItems
	return q
}

func (q *QueuePollRequest) SetAutoAck(autoAck bool) *QueuePollRequest {
	q.AutoAck = autoAck
	return q
}
func (q *QueuePollRequest) toPB() *pb.QueuesDownstreamRequest {
	return &pb.QueuesDownstreamRequest{
		RequestID:       q.RequestID,
		ClientID:        q.ClientID,
		RequestTypeData: pb.QueuesDownstreamRequestType_Get,
		Channel:         q.Channel,
		MaxItems:        q.MaxItems,
		WaitTimeout:     q.WaitTimeout,
		AutoAck:         q.AutoAck,
	}

}
