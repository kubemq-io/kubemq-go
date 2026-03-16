package kubemq

import (
	"fmt"
	"time"
)

// Response is an outbound command/query response sent by a subscriber to reply
// to a received CommandReceive or QueryReceive. It is NOT safe for concurrent
// use — create a new Response for each send operation.
//
// Fields:
//   - RequestId: the Id of the original command or query being responded to.
//     Must match CommandReceive.Id or QueryReceive.Id.
//   - ResponseTo: the internal routing address from CommandReceive.ResponseTo or
//     QueryReceive.ResponseTo. The server uses this to route the response back
//     to the original sender.
//   - Metadata: arbitrary string metadata to include in the response (typically
//     used for query responses).
//   - Body: binary response payload (typically used for query responses;
//     commands usually only need Executed/Err).
//   - ClientId: the responder's client identifier. Auto-populated from client
//     defaults if empty.
//   - ExecutedAt: timestamp when the command/query was processed.
//   - Err: if non-nil, indicates execution failure. The error message is
//     delivered to the sender in CommandResponse.Error or QueryResponse.Error.
//   - Tags: key-value pairs to include in the response.
//   - Span: OpenTelemetry span context for distributed tracing (internal use).
//
// See also: SendResponse, CommandReceive, QueryReceive.
type Response struct {
	RequestId  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientId   string
	ExecutedAt time.Time
	Err        error
	Tags       map[string]string
	Span       []byte
}

// NewResponse creates an empty Response.
func NewResponse() *Response {
	return &Response{}
}

// SetRequestId sets the corresponding request ID for this response.
func (r *Response) SetRequestId(id string) *Response {
	r.RequestId = id
	return r
}

// SetResponseTo sets the response channel.
func (r *Response) SetResponseTo(channel string) *Response {
	r.ResponseTo = channel
	return r
}

// SetMetadata sets the response metadata (for query responses).
func (r *Response) SetMetadata(metadata string) *Response {
	r.Metadata = metadata
	return r
}

// SetBody sets the response body (for query responses).
func (r *Response) SetBody(body []byte) *Response {
	r.Body = body
	return r
}

// SetTags sets the response tags.
func (r *Response) SetTags(tags map[string]string) *Response {
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
func (r *Response) SetClientId(clientId string) *Response {
	r.ClientId = clientId
	return r
}

// SetError sets the execution error for this response.
func (r *Response) SetError(err error) *Response {
	r.Err = err
	return r
}

// SetExecutedAt sets the execution timestamp.
func (r *Response) SetExecutedAt(executedAt time.Time) *Response {
	r.ExecutedAt = executedAt
	return r
}

// Validate checks all required fields and constraints.
// Called automatically before send operations; can also be called explicitly.
func (r *Response) Validate() error {
	return validateResponse(r)
}

// String returns a human-readable representation of the response.
func (r *Response) String() string {
	errStr := ""
	if r.Err != nil {
		errStr = r.Err.Error()
	}
	return fmt.Sprintf("RequestId: %s, ResponseTo: %s, Metadata: %s, Body: %s, ClientId: %s, ExecutedAt: %s, Err: %s",
		r.RequestId, r.ResponseTo, r.Metadata, string(r.Body), r.ClientId, r.ExecutedAt.String(), errStr)
}
