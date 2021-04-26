package queues

import pb "github.com/kubemq-io/protobuf/go"

type PollResponse struct {
	Messages []*QueueMessage
	*responseHandler
}

func newPollResponse(messages []*pb.QueueMessage, handler *responseHandler) *PollResponse {
	p := &PollResponse{
		Messages:        nil,
		responseHandler: handler,
	}
	for _, message := range messages {
		p.Messages = append(p.Messages, newQueueMessageFrom(message).setResponseHandler(handler))
	}
	return p
}

