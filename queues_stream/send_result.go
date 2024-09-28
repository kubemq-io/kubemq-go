package queues_stream

import (
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"strings"
)

type SendResult struct {
	Results []*pb.SendQueueMessageResult
}

func newSendResult(results []*pb.SendQueueMessageResult) *SendResult {
	s := &SendResult{
		Results: results,
	}
	return s
}

func resultToString(result *pb.SendQueueMessageResult) string {
	return fmt.Sprintf("MessageID: %s, Error: %s\n", result.MessageID, result.Error)
}
func (s *SendResult) String() string {
	sb := strings.Builder{}
	for _, result := range s.Results {
		sb.WriteString(resultToString(result))
	}
	return sb.String()
}
