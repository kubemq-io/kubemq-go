package kubemq

import (
	"context"
	"fmt"
)

type QueriesClient struct {
	client *Client
}

type QueriesSubscription struct {
	Channel  string
	Group    string
	ClientId string
}

func (qs *QueriesSubscription) Complete(opts *Options) *QueriesSubscription {
	if qs.ClientId == "" {
		qs.ClientId = opts.clientId
	}
	return qs
}
func (qs *QueriesSubscription) Validate() error {
	if qs.Channel == "" {
		return fmt.Errorf("queries subscription must have a channel")
	}
	if qs.ClientId == "" {
		return fmt.Errorf("queries subscription must have a clientId")
	}
	return nil
}
func NewQueriesClient(ctx context.Context, op ...Option) (*QueriesClient, error) {
	client, err := NewClient(ctx, op...)
	if err != nil {
		return nil, err
	}
	return &QueriesClient{
		client: client,
	}, nil
}

func (q *QueriesClient) Send(ctx context.Context, request *Query) (*QueryResponse, error) {
	if err:=q.isClientReady();err!=nil{
		return nil,err
	}
	request.transport = q.client.transport
	return q.client.SetQuery(request).Send(ctx)
}
func (q *QueriesClient) Response(ctx context.Context, response *Response) error {
	if err:=q.isClientReady();err!=nil{
		return err
	}
	response.transport = q.client.transport
	return q.client.SetResponse(response).Send(ctx)
}
func (q *QueriesClient) Subscribe(ctx context.Context, request *QueriesSubscription, onQueryReceive func(query *QueryReceive, err error)) error {
	if err:=q.isClientReady();err!=nil{
		return err
	}
	if onQueryReceive == nil {
		return fmt.Errorf("queries request subscription callback function is required")
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

func (q *QueriesClient) Close() error {
	return q.client.Close()
}


func (q *QueriesClient) isClientReady() error {
	if q.client==nil {
		return fmt.Errorf("client is not initialized")
	}
	return nil
}
