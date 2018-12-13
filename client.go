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
func (c *Client) Close() error {
	return c.grpcConn.Close()
}
func (c *Client) NewEvent(ctx context.Context) *Event {
	return &Event{
		ctx:      ctx,
		client:   c.grpcClient,
		clientId: c.opts.clientId,
	}
}
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
