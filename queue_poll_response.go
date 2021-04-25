package kubemq

import (
	pb "github.com/kubemq-io/protobuf/go"
)

type QueuePollResponse struct {
	response *pb.QueuesDownstreamResponse
}
