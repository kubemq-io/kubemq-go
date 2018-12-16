package kubemq

import (
	"context"
	"time"
)

type Response struct {
	RequestId  string
	ResponseTo string
	Metadata   string
	Body       []byte
	ClientId   string
	ExecutedAt time.Time
	Err        error
	transport  Transport
}

// SetId - set response corresponded requestId - mandatory
func (r *Response) SetRequestId(id string) *Response {
	r.RequestId = id
	return r
}

// SetResponseTo - set response channel as received in CommandReceived or QueryReceived object - mandatory
func (r *Response) SetResponseTo(channel string) *Response {
	r.ResponseTo = channel
	return r
}

// SetMetadata - set metadata response, for query only
func (r *Response) SetMetadata(metadata string) *Response {
	r.Metadata = metadata
	return r
}

// SetMetadata - set body response, for query only
func (r *Response) SetBody(body []byte) *Response {
	r.Body = body
	return r
}

// SetClientID - set clientId response, if not set default clientId will be used
func (r *Response) SetClientId(clientId string) *Response {
	r.ClientId = clientId
	return r
}

// SetError - set query or command execution error
func (r *Response) SetError(err error) *Response {
	r.Err = err
	return r
}

// SetExecutedAt - set query or command execution time
func (r *Response) SetExecutedAt(executedAt time.Time) *Response {
	r.ExecutedAt = executedAt
	return r
}

// Send - sending response to command or query request
func (r *Response) Send(ctx context.Context) error {
	return r.transport.SendResponse(ctx, r)
}
