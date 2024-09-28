package queues_stream

import (
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"sync"
	"time"
)

type QueueMessage struct {
	*pb.QueueMessage
	*responseHandler
	visibilityDuration time.Duration
	visibilityTimer    *time.Timer
	isCompleted        *atomic.Bool
	completeReason     string
	mu                 sync.Mutex
}

func (qm *QueueMessage) complete(clientId string) *QueueMessage {
	if qm.ClientID == "" {
		qm.ClientID = clientId
	}
	return qm
}

func (qm *QueueMessage) setResponseHandler(responseHandler *responseHandler) *QueueMessage {
	qm.responseHandler = responseHandler
	qm.visibilityDuration = time.Duration(responseHandler.visibilitySeconds) * time.Second
	qm.isCompleted.Store(qm.isAutoAck)
	if qm.isCompleted.Load() {
		qm.completeReason = "auto ack"
	}
	return qm
}

func NewQueueMessage() *QueueMessage {
	return &QueueMessage{
		QueueMessage: &pb.QueueMessage{
			MessageID:  "",
			ClientID:   "",
			Channel:    "",
			Metadata:   "",
			Body:       nil,
			Tags:       map[string]string{},
			Attributes: nil,
			Policy:     &pb.QueueMessagePolicy{},
		},
	}
}
func newQueueMessageFrom(msg *pb.QueueMessage) *QueueMessage {
	return &QueueMessage{
		QueueMessage: msg,
		isCompleted:  atomic.NewBool(false),
	}
}

// SetId - set queue message id, otherwise new random uuid will be set
func (qm *QueueMessage) SetId(id string) *QueueMessage {
	qm.MessageID = id
	return qm

}

// SetClientId - set queue message ClientId - mandatory if default grpcClient was not set
func (qm *QueueMessage) SetClientId(clientId string) *QueueMessage {
	qm.ClientID = clientId
	return qm
}

// SetChannel - set queue message Channel - mandatory if default Channel was not set
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

// SetTags - set key value tags to queue message
func (qm *QueueMessage) SetTags(tags map[string]string) *QueueMessage {
	qm.Tags = map[string]string{}
	for key, value := range tags {
		qm.Tags[key] = value
	}
	return qm
}

// AddTag - add key value tags to query message
func (qm *QueueMessage) AddTag(key, value string) *QueueMessage {
	if qm.Tags == nil {
		qm.Tags = map[string]string{}
	}
	qm.Tags[key] = value
	return qm
}

// SetPolicyExpirationSeconds - set queue message expiration seconds, 0 never expires
func (qm *QueueMessage) SetPolicyExpirationSeconds(sec int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.ExpirationSeconds = int32(sec)
	return qm
}

// SetPolicyDelaySeconds - set queue message delivery delay in seconds, 0 , immediate delivery
func (qm *QueueMessage) SetPolicyDelaySeconds(sec int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.DelaySeconds = int32(sec)
	return qm
}

// SetPolicyMaxReceiveCount - set max delivery attempts before message will discard or re-route to a new queue
func (qm *QueueMessage) SetPolicyMaxReceiveCount(max int) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.MaxReceiveCount = int32(max)
	return qm
}

// SetPolicyMaxReceiveQueue - set queue name to be routed once MaxReceiveCount is triggered, empty will discard the message
func (qm *QueueMessage) SetPolicyMaxReceiveQueue(channel string) *QueueMessage {
	if qm.Policy == nil {
		qm.Policy = &pb.QueueMessagePolicy{}
	}
	qm.Policy.MaxReceiveQueue = channel
	return qm
}

func (qm *QueueMessage) Ack() error {
	if qm.isCompleted.Load() {
		return fmt.Errorf("message already completed, reason: %s", qm.completeReason)
	}
	qm.isCompleted.Store(true)
	qm.completeReason = "ack"
	qm.stopVisibilityTimer()
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}

	return qm.responseHandler.AckOffsets(int64(qm.Attributes.Sequence))
}
func (qm *QueueMessage) NAck() error {
	if qm.isCompleted.Load() {
		return fmt.Errorf("message already completed, reason: %s", qm.completeReason)
	}
	qm.isCompleted.Store(true)
	qm.completeReason = "nack"
	qm.stopVisibilityTimer()
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.NAckOffsets(int64(qm.Attributes.Sequence))
}
func (qm *QueueMessage) ReQueue(channel string) error {
	if qm.isCompleted.Load() {
		return fmt.Errorf("message already completed, reason: %s", qm.completeReason)
	}
	qm.isCompleted.Store(true)
	qm.completeReason = "requeue"
	qm.stopVisibilityTimer()
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.ReQueueOffsets(channel, int64(qm.Attributes.Sequence))
}

func (qm *QueueMessage) nackOnVisibility() error {
	if qm.isCompleted.Load() {
		return fmt.Errorf("message already completed, reason: %s", qm.completeReason)
	}
	qm.isCompleted.Store(true)
	qm.completeReason = "visibility timeout"
	qm.stopVisibilityTimer()
	if qm.responseHandler == nil {
		return fmt.Errorf("function not valid")
	}
	return qm.responseHandler.NAckOffsets(int64(qm.Attributes.Sequence))
}
func (qm *QueueMessage) setCompleted(reason string) {
	qm.isCompleted.Store(true)
	qm.completeReason = reason
	qm.stopVisibilityTimer()
}
func (qm *QueueMessage) ExtendVisibility(visibilitySeconds int) error {
	if qm.isCompleted.Load() {
		return fmt.Errorf("message already completed")
	}
	if visibilitySeconds < 1 {
		return fmt.Errorf("visibility seconds must be greater than 0")
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.visibilityDuration == 0 {
		return fmt.Errorf("visibility timer not set for this message")
	}
	if qm.visibilityTimer != nil {
		isStopped := qm.visibilityTimer.Stop()
		if !isStopped {
			return fmt.Errorf("visibility timer already expired")
		}
		qm.visibilityTimer.Reset(time.Duration(visibilitySeconds) * time.Second)
	}
	return nil
}
func (qm *QueueMessage) startVisibilityTimer() {
	qm.mu.Lock()
	defer qm.mu.Unlock()

	if qm.visibilityDuration == 0 {
		return
	}
	if qm.visibilityTimer != nil {
		qm.visibilityTimer.Stop()
	}
	qm.visibilityTimer = time.AfterFunc(qm.visibilityDuration, func() {
		_ = qm.nackOnVisibility()
	})

}
func (qm *QueueMessage) stopVisibilityTimer() {
	if qm.visibilityDuration == 0 {
		return
	}
	qm.mu.Lock()
	defer qm.mu.Unlock()
	if qm.visibilityTimer != nil {
		qm.visibilityTimer.Stop()
	}
}

func (qm *QueueMessage) String() string {
	return fmt.Sprintf("Id: %s, ClientId: %s, Channel: %s, Metadata: %s, Body: %s, Tags: %s, Policy: %s, Attributes: %s",
		qm.MessageID, qm.ClientID, qm.Channel, qm.Metadata, qm.Body, qm.Tags, policyToString(qm.Policy), attributesToString(qm.Attributes))
}

func policyToString(policy *pb.QueueMessagePolicy) string {
	if policy == nil {
		return ""
	}
	return fmt.Sprintf("ExpirationSeconds: %d, DelaySeconds: %d, MaxReceiveCount: %d, MaxReceiveQueue: %s",
		policy.ExpirationSeconds, policy.DelaySeconds, policy.MaxReceiveCount, policy.MaxReceiveQueue)
}

func attributesToString(attributes *pb.QueueMessageAttributes) string {
	if attributes == nil {
		return ""
	}
	return fmt.Sprintf("Sequence: %d, Timestamp: %s, ReceiveCount: %d, ReRouted: %t, ReRoutedFromQueue: %s, ExpirationAt: %s, DelayedTo: %s",
		attributes.Sequence, time.Unix(attributes.Timestamp, 0).String(), attributes.ReceiveCount, attributes.ReRouted, attributes.ReRoutedFromQueue, time.Unix(attributes.ExpirationAt, 0).String(), time.Unix(attributes.DelayedTo, 0).String())
}
