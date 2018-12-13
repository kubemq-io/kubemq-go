package kubemq

import (
	"context"
	"fmt"
	"github.com/kubemq-io/go/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Client struct {
	opts       *Options
	grpcConn   *grpc.ClientConn
	grpcClient pb.KubemqClient
}

func getGrpcConn(ctx context.Context, opts *Options) (conn *grpc.ClientConn, err error) {
	var connOptions []grpc.DialOption
	if opts.isSecured {
		creds, err := credentials.NewClientTLSFromFile(opts.certFile, opts.serverOverrideDomain)
		if err != nil {
			return nil, fmt.Errorf("could not load tls cert: %s", err)
		}
		connOptions = append(connOptions, grpc.WithTransportCredentials(creds))
	} else {
		connOptions = append(connOptions, grpc.WithInsecure())
	}
	address := fmt.Sprintf("%s:%d", opts.host, opts.port)
	conn, err = grpc.DialContext(ctx, address, connOptions...)
	if err != nil {
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	return conn, nil

}

// NewClient - creat client instance to be use to communicate with KubeMQ server
func NewClient(ctx context.Context, op ...Option) (*Client, error) {
	opts := GetDefaultOptions()
	for _, o := range op {
		o.apply(opts)
	}

	conn, err := getGrpcConn(ctx, opts)
	if err != nil {
		return nil, err
	}

	client := &Client{
		opts:       opts,
		grpcConn:   conn,
		grpcClient: pb.NewKubemqClient(conn),
	}
	return client, nil
}

// Close - closing client connection. any on going transactions will be aborted
func (c *Client) Close() error {
	return c.grpcConn.Close()
}

// NewEvent - create an event object
func (c *Client) NewEvent(ctx context.Context) *Event {
	return &Event{
		ctx:      ctx,
		client:   c.grpcClient,
		clientId: c.opts.clientId,
	}
}

// StreamEvents - send stream of events in a single call
func (c *Client) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.grpcClient.SendEventsStream(streamCtx)
	if err != nil {
		errCh <- err
		return
	}
	defer stream.CloseSend()
	go func() {
		for {
			result, err := stream.Recv()
			if err != nil {
				errCh <- err
				cancel()
				return
			}
			if !result.Sent {
				errCh <- fmt.Errorf("%s", result.Error)
			}
		}
	}()

	for {
		select {
		case event := <-eventsCh:
			err := stream.Send(&pb.Event{
				EventID:  event.EventID,
				ClientID: c.opts.clientId,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
			})
			if err != nil {
				errCh <- err
				return
			}
		case <-ctx.Done():
			return
		}
	}

}

// SubscribeToEvents - subscribe to events by channel and group. return channel of events or en error
func (c *Client) SubscribeToEvents(ctx context.Context, channel, group string, errCh chan error) (<-chan *Event, error) {
	eventsCh := make(chan *Event, c.opts.receiveBufferSize)
	subRequest := &pb.Subscribe{
		SubscribeTypeData: pb.Events,
		ClientID:          c.opts.clientId,
		Channel:           channel,
		Group:             group,
	}
	stream, err := c.grpcClient.SubscribeToEvents(ctx, subRequest)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				errCh <- err
				close(eventsCh)
				return
			}
			eventsCh <- &Event{
				EventID:  event.EventID,
				Channel:  event.Channel,
				Metadata: event.Metadata,
				Body:     event.Body,
			}
		}
	}()
	return eventsCh, nil
}
