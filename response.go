package kubemq

import (
	"fmt"
	"time"
)

// CommandReply is an outbound command response sent by a subscriber to reply
// to a received CommandReceive. It is NOT safe for concurrent use — create a
// new CommandReply for each send operation.
//
// Fields:
//   - RequestId: the Id of the original command being responded to.
//     Must match CommandReceive.Id.
//   - ResponseTo: the internal routing address from CommandReceive.ResponseTo.
//     The server uses this to route the response back to the original sender.
//   - Metadata: arbitrary string metadata to include in the response.
//   - Body: binary response payload.
//   - ClientId: the responder's client identifier. Auto-populated from client
//     defaults if empty.
//   - ExecutedAt: timestamp when the command was processed.
//   - Err: if non-nil, indicates execution failure. The error message is
//     delivered to the sender in CommandResponse.Error.
//   - Tags: key-value pairs to include in the response.
//
// See also: SendCommandResponse, CommandReceive.
type CommandReply struct {
	RequestId  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientId   string
	ExecutedAt time.Time
	Err        error
	Tags       map[string]string
}

// NewCommandReply creates an empty CommandReply.
func NewCommandReply() *CommandReply {
	return &CommandReply{}
}

// SetRequestId sets the corresponding request ID for this response.
func (r *CommandReply) SetRequestId(id string) *CommandReply {
	r.RequestId = id
	return r
}

// SetResponseTo sets the response channel.
func (r *CommandReply) SetResponseTo(channel string) *CommandReply {
	r.ResponseTo = channel
	return r
}

// SetMetadata sets the response metadata.
func (r *CommandReply) SetMetadata(metadata string) *CommandReply {
	r.Metadata = metadata
	return r
}

// SetBody sets the response body.
func (r *CommandReply) SetBody(body []byte) *CommandReply {
	r.Body = body
	return r
}

// SetTags sets the response tags.
func (r *CommandReply) SetTags(tags map[string]string) *CommandReply {
	if tags == nil {
		r.Tags = nil
		return r
	}
	cp := make(map[string]string, len(tags))
	for k, v := range tags {
		cp[k] = v
	}
	r.Tags = cp
	return r
}

// SetClientId sets the client identifier for this response.
func (r *CommandReply) SetClientId(clientId string) *CommandReply {
	r.ClientId = clientId
	return r
}

// SetError sets the execution error for this response.
func (r *CommandReply) SetError(err error) *CommandReply {
	r.Err = err
	return r
}

// SetExecutedAt sets the execution timestamp.
func (r *CommandReply) SetExecutedAt(executedAt time.Time) *CommandReply {
	r.ExecutedAt = executedAt
	return r
}

// Validate checks all required fields and constraints.
func (r *CommandReply) Validate() error {
	return validateCommandReply(r)
}

// String returns a human-readable representation of the command reply.
func (r *CommandReply) String() string {
	errStr := ""
	if r.Err != nil {
		errStr = r.Err.Error()
	}
	return fmt.Sprintf("RequestId: %s, ResponseTo: %s, Metadata: %s, Body: %s, ClientId: %s, ExecutedAt: %s, Err: %s",
		r.RequestId, r.ResponseTo, r.Metadata, string(r.Body), r.ClientId, r.ExecutedAt.String(), errStr)
}

// QueryReply is an outbound query response sent by a subscriber to reply
// to a received QueryReceive. It is NOT safe for concurrent use — create a
// new QueryReply for each send operation.
//
// Fields:
//   - RequestId: the Id of the original query being responded to.
//     Must match QueryReceive.Id.
//   - ResponseTo: the internal routing address from QueryReceive.ResponseTo.
//     The server uses this to route the response back to the original sender.
//   - Metadata: arbitrary string metadata to include in the response.
//   - Body: binary response payload.
//   - ClientId: the responder's client identifier. Auto-populated from client
//     defaults if empty.
//   - ExecutedAt: timestamp when the query was processed.
//   - Err: if non-nil, indicates execution failure. The error message is
//     delivered to the sender in QueryResponse.Error.
//   - Tags: key-value pairs to include in the response.
//   - CacheHit: reserved for query response caching (set by the server).
//
// See also: SendQueryResponse, QueryReceive.
type QueryReply struct {
	RequestId  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientId   string
	ExecutedAt time.Time
	Err        error
	Tags       map[string]string
	CacheHit   bool
}

// NewQueryReply creates an empty QueryReply.
func NewQueryReply() *QueryReply {
	return &QueryReply{}
}

// SetRequestId sets the corresponding request ID for this response.
func (r *QueryReply) SetRequestId(id string) *QueryReply {
	r.RequestId = id
	return r
}

// SetResponseTo sets the response channel.
func (r *QueryReply) SetResponseTo(channel string) *QueryReply {
	r.ResponseTo = channel
	return r
}

// SetMetadata sets the response metadata.
func (r *QueryReply) SetMetadata(metadata string) *QueryReply {
	r.Metadata = metadata
	return r
}

// SetBody sets the response body.
func (r *QueryReply) SetBody(body []byte) *QueryReply {
	r.Body = body
	return r
}

// SetTags sets the response tags.
func (r *QueryReply) SetTags(tags map[string]string) *QueryReply {
	if tags == nil {
		r.Tags = nil
		return r
	}
	cp := make(map[string]string, len(tags))
	for k, v := range tags {
		cp[k] = v
	}
	r.Tags = cp
	return r
}

// SetClientId sets the client identifier for this response.
func (r *QueryReply) SetClientId(clientId string) *QueryReply {
	r.ClientId = clientId
	return r
}

// SetError sets the execution error for this response.
func (r *QueryReply) SetError(err error) *QueryReply {
	r.Err = err
	return r
}

// SetExecutedAt sets the execution timestamp.
func (r *QueryReply) SetExecutedAt(executedAt time.Time) *QueryReply {
	r.ExecutedAt = executedAt
	return r
}

// Validate checks all required fields and constraints.
func (r *QueryReply) Validate() error {
	return validateQueryReply(r)
}

// String returns a human-readable representation of the query reply.
func (r *QueryReply) String() string {
	errStr := ""
	if r.Err != nil {
		errStr = r.Err.Error()
	}
	return fmt.Sprintf("RequestId: %s, ResponseTo: %s, Metadata: %s, Body: %s, ClientId: %s, ExecutedAt: %s, Err: %s, CacheHit: %v",
		r.RequestId, r.ResponseTo, r.Metadata, string(r.Body), r.ClientId, r.ExecutedAt.String(), errStr, r.CacheHit)
}
