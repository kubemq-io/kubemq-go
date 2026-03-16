package transport

import (
	"time"

	pb "github.com/kubemq-io/kubemq-go/v2/pb"
)

// EventStreamItemToProto converts an EventStreamItem to a protobuf Event.
func EventStreamItemToProto(item *EventStreamItem, clientID string) *pb.Event {
	return &pb.Event{
		EventID:  item.ID,
		ClientID: firstNonEmpty(item.ClientID, clientID),
		Channel:  item.Channel,
		Metadata: item.Metadata,
		Body:     item.Body,
		Store:    item.Store,
		Tags:     item.Tags,
	}
}

// EventStreamResultFromProto converts a protobuf Result to an EventStreamResult.
func EventStreamResultFromProto(r *pb.Result) *EventStreamResult {
	if r == nil {
		return nil
	}
	return &EventStreamResult{
		EventID: r.EventID,
		Sent:    r.Sent,
		Error:   r.Error,
	}
}

// EventToProto converts an internal SendEventRequest to a protobuf Event.
func EventToProto(req *SendEventRequest, clientID string) *pb.Event {
	return &pb.Event{
		EventID:  req.ID,
		ClientID: firstNonEmpty(req.ClientID, clientID),
		Channel:  req.Channel,
		Metadata: req.Metadata,
		Body:     req.Body,
		Store:    false,
		Tags:     req.Tags,
	}
}

// EventStoreToProto converts an internal SendEventStoreRequest to a protobuf Event with Store=true.
func EventStoreToProto(req *SendEventStoreRequest, clientID string) *pb.Event {
	return &pb.Event{
		EventID:  req.ID,
		ClientID: firstNonEmpty(req.ClientID, clientID),
		Channel:  req.Channel,
		Metadata: req.Metadata,
		Body:     req.Body,
		Store:    true,
		Tags:     req.Tags,
	}
}

// EventStoreResultFromProto converts a protobuf Result to an internal SendEventStoreResult.
func EventStoreResultFromProto(r *pb.Result) *SendEventStoreResult {
	if r == nil {
		return &SendEventStoreResult{}
	}
	return &SendEventStoreResult{
		ID:   r.EventID,
		Sent: r.Sent,
	}
}

// CommandToProto converts an internal SendCommandRequest to a protobuf Request.
func CommandToProto(req *SendCommandRequest, clientID string) *pb.Request {
	return &pb.Request{
		RequestID:       req.ID,
		RequestTypeData: pb.Request_Command,
		ClientID:        firstNonEmpty(req.ClientID, clientID),
		Channel:         req.Channel,
		Metadata:        req.Metadata,
		Body:            req.Body,
		Timeout:         int32(req.Timeout.Milliseconds()), //nolint:gosec // G115: timeout in ms fits int32 (max ~24 days)
		Tags:            req.Tags,
		Span:            req.Span,
	}
}

// CommandResultFromProto converts a protobuf Response to an internal SendCommandResult.
func CommandResultFromProto(r *pb.Response) *SendCommandResult {
	if r == nil {
		return &SendCommandResult{}
	}
	return &SendCommandResult{
		CommandID:        r.RequestID,
		ResponseClientID: r.ClientID,
		Executed:         r.Executed,
		ExecutedAt:       time.Unix(0, r.Timestamp),
		Error:            r.Error,
		Tags:             r.Tags,
	}
}

// QueryToProto converts an internal SendQueryRequest to a protobuf Request.
func QueryToProto(req *SendQueryRequest, clientID string) *pb.Request {
	return &pb.Request{
		RequestID:       req.ID,
		RequestTypeData: pb.Request_Query,
		ClientID:        firstNonEmpty(req.ClientID, clientID),
		Channel:         req.Channel,
		Metadata:        req.Metadata,
		Body:            req.Body,
		Timeout:         int32(req.Timeout.Milliseconds()), //nolint:gosec // G115: timeout in ms fits int32 (max ~24 days)
		CacheKey:        req.CacheKey,
		CacheTTL:        int32(req.CacheTTL.Seconds()),
		Tags:            req.Tags,
		Span:            req.Span,
	}
}

// QueryResultFromProto converts a protobuf Response to an internal SendQueryResult.
func QueryResultFromProto(r *pb.Response) *SendQueryResult {
	if r == nil {
		return &SendQueryResult{}
	}
	return &SendQueryResult{
		QueryID:          r.RequestID,
		Executed:         r.Executed,
		ExecutedAt:       time.Unix(0, r.Timestamp),
		Metadata:         r.Metadata,
		ResponseClientID: r.ClientID,
		Body:             r.Body,
		CacheHit:         r.CacheHit,
		Error:            r.Error,
		Tags:             r.Tags,
	}
}

// ResponseToProto converts an internal SendResponseRequest to a protobuf Response.
func ResponseToProto(req *SendResponseRequest) *pb.Response {
	errStr := ""
	if req.Err != nil {
		errStr = req.Err.Error()
	}
	return &pb.Response{
		ClientID:     req.ClientID,
		RequestID:    req.RequestID,
		ReplyChannel: req.ResponseTo,
		Metadata:     req.Metadata,
		Body:         req.Body,
		Timestamp:    req.ExecutedAt.UnixNano(),
		Executed:     req.Err == nil,
		Error:        errStr,
		Tags:         req.Tags,
		Span:         req.Span,
	}
}

// QueueMessageToProto converts an internal QueueMessageItem to a protobuf QueueMessage.
func QueueMessageToProto(m *QueueMessageItem, clientID string) *pb.QueueMessage {
	msg := &pb.QueueMessage{
		MessageID: m.ID,
		ClientID:  firstNonEmpty(m.ClientID, clientID),
		Channel:   m.Channel,
		Metadata:  m.Metadata,
		Body:      m.Body,
		Tags:      m.Tags,
	}
	if m.Policy != nil {
		msg.Policy = &pb.QueueMessagePolicy{
			ExpirationSeconds: int32(m.Policy.ExpirationSeconds), //nolint:gosec // G115: bounded policy values fit int32
			DelaySeconds:      int32(m.Policy.DelaySeconds),      //nolint:gosec // G115: bounded policy values fit int32
			MaxReceiveCount:   int32(m.Policy.MaxReceiveCount),   //nolint:gosec // G115: bounded policy values fit int32
			MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
		}
	}
	return msg
}

// QueueMessageFromProto converts a protobuf QueueMessage to an internal QueueMessageItem.
func QueueMessageFromProto(m *pb.QueueMessage) *QueueMessageItem {
	if m == nil {
		return nil
	}
	item := &QueueMessageItem{
		ID:       m.MessageID,
		ClientID: m.ClientID,
		Channel:  m.Channel,
		Metadata: m.Metadata,
		Body:     m.Body,
		Tags:     m.Tags,
	}
	if m.Policy != nil {
		item.Policy = &QueueMessagePolicy{
			ExpirationSeconds: int(m.Policy.ExpirationSeconds),
			DelaySeconds:      int(m.Policy.DelaySeconds),
			MaxReceiveCount:   int(m.Policy.MaxReceiveCount),
			MaxReceiveQueue:   m.Policy.MaxReceiveQueue,
		}
	}
	if m.Attributes != nil {
		item.Attributes = &QueueMessageAttributes{
			Timestamp:         m.Attributes.Timestamp,
			Sequence:          m.Attributes.Sequence,
			MD5OfBody:         m.Attributes.MD5OfBody,
			ReceiveCount:      int(m.Attributes.ReceiveCount),
			ReRouted:          m.Attributes.ReRouted,
			ReRoutedFromQueue: m.Attributes.ReRoutedFromQueue,
			ExpirationAt:      m.Attributes.ExpirationAt,
			DelayedTo:         m.Attributes.DelayedTo,
		}
	}
	return item
}

// ServerInfoFromProto converts a protobuf PingResult to an internal ServerInfoResult.
func ServerInfoFromProto(r *pb.PingResult) *ServerInfoResult {
	if r == nil {
		return nil
	}
	return &ServerInfoResult{
		Host:                r.Host,
		Version:             r.Version,
		ServerStartTime:     r.ServerStartTime,
		ServerUpTimeSeconds: r.ServerUpTimeSeconds,
	}
}

// SendQueueMessageResultFromProto converts a protobuf SendQueueMessageResult.
func SendQueueMessageResultFromProto(r *pb.SendQueueMessageResult) *SendQueueMessageResultItem {
	if r == nil {
		return nil
	}
	return &SendQueueMessageResultItem{
		MessageID:    r.MessageID,
		SentAt:       r.SentAt,
		ExpirationAt: r.ExpirationAt,
		DelayedTo:    r.DelayedTo,
		IsError:      r.IsError,
		Error:        r.Error,
	}
}

// ReceiveQueueMessagesRespFromProto converts a protobuf ReceiveQueueMessagesResponse.
func ReceiveQueueMessagesRespFromProto(r *pb.ReceiveQueueMessagesResponse) *ReceiveQueueMessagesResp {
	if r == nil {
		return nil
	}
	msgs := make([]*QueueMessageItem, 0, len(r.Messages))
	for _, m := range r.Messages {
		msgs = append(msgs, QueueMessageFromProto(m))
	}
	return &ReceiveQueueMessagesResp{
		RequestID:        r.RequestID,
		Messages:         msgs,
		MessagesReceived: r.MessagesReceived,
		MessagesExpired:  r.MessagesExpired,
		IsPeak:           r.IsPeak,
		IsError:          r.IsError,
		Error:            r.Error,
	}
}

// AckAllRespFromProto converts a protobuf AckAllQueueMessagesResponse.
func AckAllRespFromProto(r *pb.AckAllQueueMessagesResponse) *AckAllQueueMessagesResp {
	if r == nil {
		return nil
	}
	return &AckAllQueueMessagesResp{
		RequestID:        r.RequestID,
		AffectedMessages: r.AffectedMessages,
		IsError:          r.IsError,
		Error:            r.Error,
	}
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// EventReceiveFromProto converts a protobuf EventReceive to an SDK Event (for non-store subscriptions).
func EventReceiveFromProto(e *pb.EventReceive) *EventReceiveItem {
	if e == nil {
		return nil
	}
	return &EventReceiveItem{
		ID:        e.EventID,
		Channel:   e.Channel,
		Metadata:  e.Metadata,
		Body:      e.Body,
		Timestamp: e.Timestamp,
		Tags:      e.Tags,
	}
}

// EventStoreReceiveFromProto converts a protobuf EventReceive to an EventStoreReceiveItem.
func EventStoreReceiveFromProto(e *pb.EventReceive) *EventStoreReceiveItem {
	if e == nil {
		return nil
	}
	return &EventStoreReceiveItem{
		ID:        e.EventID,
		Channel:   e.Channel,
		Metadata:  e.Metadata,
		Body:      e.Body,
		Timestamp: e.Timestamp,
		Sequence:  e.Sequence,
		Tags:      e.Tags,
	}
}

// CommandReceiveFromProto converts a protobuf Request to a CommandReceiveItem.
func CommandReceiveFromProto(r *pb.Request) *CommandReceiveItem {
	if r == nil {
		return nil
	}
	return &CommandReceiveItem{
		ID:         r.RequestID,
		ClientID:   r.ClientID,
		Channel:    r.Channel,
		Metadata:   r.Metadata,
		Body:       r.Body,
		ResponseTo: r.ReplyChannel,
		Tags:       r.Tags,
		Span:       r.Span,
	}
}

// QueryReceiveFromProto converts a protobuf Request to a QueryReceiveItem.
func QueryReceiveFromProto(r *pb.Request) *QueryReceiveItem {
	if r == nil {
		return nil
	}
	return &QueryReceiveItem{
		ID:         r.RequestID,
		ClientID:   r.ClientID,
		Channel:    r.Channel,
		Metadata:   r.Metadata,
		Body:       r.Body,
		ResponseTo: r.ReplyChannel,
		Tags:       r.Tags,
		Span:       r.Span,
	}
}

// QueueUpstreamResponseFromProto converts a protobuf QueuesUpstreamResponse to a QueueUpstreamResult.
func QueueUpstreamResponseFromProto(r *pb.QueuesUpstreamResponse) *QueueUpstreamResult {
	if r == nil {
		return nil
	}
	results := make([]*SendQueueMessageResultItem, 0, len(r.Results))
	for _, res := range r.Results {
		results = append(results, &SendQueueMessageResultItem{
			MessageID:    res.MessageID,
			SentAt:       res.SentAt,
			ExpirationAt: res.ExpirationAt,
			DelayedTo:    res.DelayedTo,
			IsError:      res.IsError,
			Error:        res.Error,
		})
	}
	return &QueueUpstreamResult{
		RefRequestID: r.RefRequestID,
		Results:      results,
		IsError:      r.IsError,
		Error:        r.Error,
	}
}

// QueueDownstreamResponseFromProto converts a protobuf QueuesDownstreamResponse to a QueueDownstreamResult.
func QueueDownstreamResponseFromProto(r *pb.QueuesDownstreamResponse) *QueueDownstreamResult {
	if r == nil {
		return nil
	}
	msgs := make([]*QueueMessageItem, 0, len(r.Messages))
	for _, m := range r.Messages {
		msgs = append(msgs, QueueMessageFromProto(m))
	}
	return &QueueDownstreamResult{
		TransactionID:       r.TransactionId,
		RefRequestID:        r.RefRequestId,
		Messages:            msgs,
		ActiveOffsets:       r.ActiveOffsets,
		IsError:             r.IsError,
		Error:               r.Error,
		TransactionComplete: r.TransactionComplete,
		Metadata:            r.Metadata,
	}
}
