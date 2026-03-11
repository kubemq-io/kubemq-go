package kubemq

import (
	"fmt"
	"time"
)

// Response is an outbound command/query response. It is NOT safe for concurrent
// use — create a new Response for each send operation.
type Response struct {
	RequestId  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientId   string
	ExecutedAt time.Time
	Err        error
	Tags       map[string]string
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
	r.Tags = tags
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
