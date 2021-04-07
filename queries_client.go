package kubemq

import (
	"context"
	"fmt"
)

type QueriesMessageHandler func(receive *QueryReceive)
type QueriesErrorsHandler func(error)

type QueriesClient struct {
	client *Client
}

type QueriesSubscription struct {
	Channel string
	Group   string
	QueriesMessageHandler
	QueriesErrorsHandler
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

func (c *QueriesClient) Send(ctx context.Context, request *Query) (*QueryResponse, error) {
	return c.client.SetQuery(request).Send(ctx)
}
func (c *QueriesClient) Response(ctx context.Context, response *Response) error {
	return c.client.SetResponse(response).Send(ctx)
}
func (c *QueriesClient) Subscribe(ctx context.Context, channel, group string, onQueryReceive func(query *QueryReceive, err error)) error {
	if onQueryReceive == nil {
		return fmt.Errorf("queries request subscription callback function is required")
	}
	errCh := make(chan error, 1)
	queriesCh, err := c.client.SubscribeToQueries(ctx, channel, group, errCh)
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

func (c *QueriesClient) Close() error {
	return c.client.Close()
}
