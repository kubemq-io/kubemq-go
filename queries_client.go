package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
)

// QueriesClient represents a client for making queries to a server.
// It contains a reference to the underlying client that handles the
// communication with the server.
type QueriesClient struct {
	client *Client
}

// QueriesSubscription represents a subscription to queries requests by channel and group
type QueriesSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

// Complete updates the ClientId of the QueriesSubscription if it is empty,
// using the clientId value from the provided Options.
// It returns a pointer to the modified QueriesSubscription.
func (qs *QueriesSubscription) Complete(opts *Options) *QueriesSubscription {
	if qs.ClientId == "" {
		qs.ClientId = opts.clientId
	}
	return qs
}

// Validate checks if a queries subscription is valid. It returns an error if any
// of the required fields are empty.
func (qs *QueriesSubscription) Validate() error {
	if qs.Channel == "" {
		return fmt.Errorf("queries subscription must have a channel")
	}
	if qs.ClientId == "" {
		return fmt.Errorf("queries subscription must have a clientId")
	}
	return nil
}

// NewQueriesClient creates a new instance of QueriesClient by calling NewClient function and returning QueriesClient
// with the newly created Client instance. It receives a context and an optional list of options as arguments
// and returns a pointer to QueriesClient and an error.
func NewQueriesClient(ctx context.Context, op ...Option) (*QueriesClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueriesClient{
		client: client,
	}, nil
}

// Send sends a query request using the provided context. It checks if the client is ready,
// sets the transport from the client, and calls the Send method on the client with the request.
// It returns the query response and an error, if any.
//
// The following fields in the request are required:
// - transport (set from the client)
//
// Example usage:
//
//	request := &Query{
//	    Id:        "123",
//	    Channel:   "channel1",
//	    Metadata:  "metadata",
//	    Body:      []byte("query body"),
//	    Timeout:   time.Second,
//	    ClientId:  "client1",
//	    CacheKey:  "cacheKey",
//	    CacheTTL:  time.Minute,
//	    Tags:      map[string]string{"tag1": "value1"},
//	}
//	response, err := client.Send(ctx, request)
func (q *QueriesClient) Send(ctx context.Context, request *Query) (*QueryResponse, error) {
	if err := q.isClientReady(); err != nil {
		return nil, err
	}
	request.transport = q.client.transport
	return q.client.SetQuery(request).Send(ctx)
}

// Response sends a response to a command or query request.
//
// The response must have a corresponding requestId and response channel,
// which are set using SetRequestId and SetResponseTo methods, respectively.
// The requestId is mandatory, while the response channel is received from
// either CommandReceived or QueryReceived objects.
//
// Additional optional attributes that can be set for the response include:
//   - Metadata: SetMetadata should be used to set metadata for a query response only.
//   - Body: SetBody should be used to set the body for a query response only.
//   - Tags: SetTags can be used to set tags for the response.
//   - ClientId: SetClientId can be used to set the clientId for the response. If not set,
//     the default clientId will be used.
//   - Error: SetError can be used to set an error when executing a command or query.
//   - ExecutedAt: SetExecutedAt can be used to set the execution time for a command or query.
//   - Trace: AddTrace can be used to add tracing support to the response.
//
// Once all the necessary attributes are set, call the Send method to send the response.
//
// Example:
//
//	resp := &Response{}
//	resp.SetRequestId("12345")
//	resp.SetResponseTo("channel-name")
//	resp.SetMetadata("metadata")
//	resp.SetBody([]byte("response-body"))
//	resp.SetTags(map[string]string{"tag1": "value1", "tag2": "value2"})
//	resp.SetClientId("client-id")
//	resp.SetError(errors.New("some error"))
//	resp.SetExecutedAt(time.Now())
//	resp.AddTrace("trace-name")
//
//	err := resp.Send(ctx)
//	if err != nil {
//	  log.Println("Failed to send response:", err)
//	}
func (q *QueriesClient) Response(ctx context.Context, response *Response) error {
	if err := q.isClientReady(); err != nil {
		return err
	}
	response.transport = q.client.transport
	return q.client.SetResponse(response).Send(ctx)
}

// Subscribe is a method of QueriesClient that allows a client to subscribe to queries.
// It takes a context, a QueriesSubscription request, and a callback function onQueryReceive.
// The context is used for cancellation, timing out, and passing values between middleware.
// The QueriesSubscription defines the channel, group, and clientId for the subscription.
// The onQueryReceive callback function will be called when a query is received or an error occurs.
// The method returns an error if the client is not ready, if the onQueryReceive function is nil,
// or if the QueriesSubscription request is invalid.
// The method creates a channel for receiving errors, subscribes to queries with the given request,
// and starts a goroutine that continuously listens for new queries or errors on the channel.
// When a query is received, it is passed to the onQueryReceive function with a nil error.
// When an error is received, it is passed to the onQueryReceive function with a nil query.
// If the context is canceled, the goroutine returns.
// The method returns with nil if the subscription is successfully set up.
func (q *QueriesClient) Subscribe(ctx context.Context, request *QueriesSubscription, onQueryReceive func(query *QueryReceive, err error)) error {
	if err := q.isClientReady(); err != nil {
		return err
	}
	if onQueryReceive == nil {
		return fmt.Errorf("queries request subscription callback function is required")
	}
	if err := request.Complete(q.client.opts).Validate(); err != nil {
		return err
	}

	errCh := make(chan error, 1)
	queriesCh, err := q.client.SubscribeToQueriesWithRequest(ctx, request, errCh)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case query := <-queriesCh:
				onQueryReceive(query, nil)
			case err := <-errCh:
				onQueryReceive(nil, err)
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Create creates a new channel in the QueriesClient with the given channel name.
//
// Parameters:
//   - ctx (context.Context): The context for the request.
//   - channel (string): The name of the channel to create.
//
// Returns:
//   - error: An error if the channel creation fails.
func (q *QueriesClient) Create(ctx context.Context, channel string) error {
	return CreateChannel(ctx, q.client, q.client.opts.clientId, channel, "queries")
}

// Delete deletes a channel.
//
// The method receives a context and the channel name to be deleted. It invokes the DeleteChannel function passing the
// received channel name as well as the clientId and channelType. DeleteChannel creates a new Query instance and sets
// the necessary properties such as the channel, clientId, metadata, tags, and timeout. It then calls the Send method
// of the client to send the delete channel request. If an error occurs during the request execution, it returns an
// error. If the response contains an error message, it returns an error. Otherwise, it returns nil, indicating the
// channel was successfully deleted.
func (q *QueriesClient) Delete(ctx context.Context, channel string) error {
	return DeleteChannel(ctx, q.client, q.client.opts.clientId, channel, "queries")
}

// List retrieves a list of channels with the specified search criteria.
// It returns a slice of *common.CQChannel and an error.
// The search criteria is passed as a string parameter.
// The Channels are retrieved using the ListCQChannels function, passing the context, client, client ID, channel type, and search criteria.
// If an error occurs during the retrieval, it is returned.
// If the retrieval is successful, the data is decoded into a slice of *common.CQChannel using the DecodeCQChannelList function.
// The decoded data and any error are returned.
func (q *QueriesClient) List(ctx context.Context, search string) ([]*common.CQChannel, error) {
	return ListCQChannels(ctx, q.client, q.client.opts.clientId, "queries", search)
}

// Close closes the QueriesClient's underlying client connection.
// This method returns an error if the client is not initialized.
func (q *QueriesClient) Close() error {
	return q.client.Close()
}

// isClientReady checks if the client is properly initialized.
// It returns an error if the client is nil, indicating that it is not initialized.
func (q *QueriesClient) isClientReady() error {
	if q.client == nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
