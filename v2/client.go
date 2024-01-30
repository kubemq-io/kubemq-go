package v2

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/config"
	"github.com/kubemq-io/kubemq-go/v2/grpc"
	"github.com/kubemq-io/kubemq-go/v2/pubsub/events"
	"github.com/kubemq-io/kubemq-go/v2/pubsub/events_store"
	"time"
)

type Client struct {
	cfg               *config.Connection
	clientCtx         context.Context
	clientCancelFunc  context.CancelFunc
	transport         *grpc.Transport
	eventsClient      *events.EventsClient
	eventsStoreClient *events_store.EventsStoreClient
}

func NewClient(ctx context.Context, cfg *config.Connection) (*Client, error) {
	if err := cfg.Complete().Validate(); err != nil {
		return nil, fmt.Errorf("error validating connection configuration, %w", err)
	}
	c := &Client{
		cfg: cfg,
	}
	c.clientCtx, c.clientCancelFunc = context.WithCancel(ctx)
	var err error
	c.transport, err = grpc.NewTransport(c.clientCtx, cfg)
	if err != nil {
		return nil, err
	}
	c.eventsClient = events.NewEventsClient(c.transport.KubeMQClient(), cfg.ClientId)
	c.eventsStoreClient = events_store.NewEventsStoreClient(c.transport.KubeMQClient(), cfg.ClientId)
	return c, nil
}

func (c *Client) SendEvent(ctx context.Context, event *events.Event) error {
	if c.clientCtx.Err() != nil {
		return fmt.Errorf("client is closed")
	}
	err := c.eventsClient.Send(ctx, event)
	return err
}

func (c *Client) SubscribeToEvents(ctx context.Context, subscription *events.EventsSubscription) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if c.clientCtx.Err() != nil {
		return fmt.Errorf("client is closed")
	}
	subscriberCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			errCh := make(chan error, 1)
			c.eventsClient.Subscribe(subscriberCtx, subscription, errCh)
			select {
			case <-c.clientCtx.Done():
				cancel()
				return
			case <-subscriberCtx.Done():
				return
			case err := <-errCh:
				if subscription.OnError != nil {
					subscription.OnError(err)
				}
				if c.cfg.DisableAutoReconnect {
					return
				}
				time.Sleep(c.cfg.GetReconnectIntervalDuration())
			}
		}
	}()
	return nil
}

func (c *Client) SendEventStore(ctx context.Context, event *events_store.EventStore) error {
	if c.clientCtx.Err() != nil {
		return fmt.Errorf("client is closed")
	}
	err := c.eventsStoreClient.Send(ctx, event)
	return err
}

func (c *Client) SubscribeToEventsStore(ctx context.Context, subscription *events_store.EventStoreSubscription) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if c.clientCtx.Err() != nil {
		return fmt.Errorf("client is closed")
	}
	subscriberCtx, cancel := context.WithCancel(ctx)
	go func() {
		for {
			errCh := make(chan error, 1)
			c.eventsStoreClient.Subscribe(subscriberCtx, subscription, errCh)
			select {
			case <-c.clientCtx.Done():
				cancel()
				return
			case <-subscriberCtx.Done():
				return
			case err := <-errCh:
				if subscription.OnError != nil {
					subscription.OnError(err)
				}
				if c.cfg.DisableAutoReconnect {
					return
				}
				time.Sleep(c.cfg.GetReconnectIntervalDuration())
			}
		}
	}()
	return nil
}

func (c *Client) Ping(ctx context.Context) (*grpc.ServerInfo, error) {
	if c.clientCtx.Err() != nil {
		return nil, fmt.Errorf("client is closed")
	}
	return c.transport.Ping(ctx)
}

func (c *Client) Close() error {
	c.clientCancelFunc()
	return c.transport.Close()
}
