package grpc

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/v2/config"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Transport struct {
	opts     *config.Connection
	conn     *grpc.ClientConn
	client   pb.KubemqClient
	isClosed *atomic.Bool
}

func NewTransport(ctx context.Context, cfg *config.Connection) (*Transport, error) {
	t := &Transport{
		opts:     cfg,
		conn:     nil,
		client:   nil,
		isClosed: atomic.NewBool(false),
	}

	connOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(cfg.MaxReceiveSize), grpc.MaxCallSendMsgSize(cfg.MaxSendSize)),
		grpc.WithUnaryInterceptor(t.setUnaryInterceptor()),
		grpc.WithStreamInterceptor(t.setStreamInterceptor()),
	}
	tlsOptions, err := getTLSConnectionOptions(cfg.Tls)
	if err != nil {
		return nil, err
	}
	if tlsOptions != nil {
		connOptions = append(connOptions, tlsOptions...)
	}
	if keepAliveOptions := getKeepAliveConnectionOptions(cfg.KeepAlive); keepAliveOptions != nil {
		connOptions = append(connOptions, keepAliveOptions...)
	}

	t.conn, err = grpc.DialContext(ctx, cfg.Address, connOptions...)
	if err != nil {
		return nil, fmt.Errorf("error connecting to kubemq server, %w", err)
	}
	go func() {
		<-ctx.Done()
		if t.conn != nil {
			_ = t.conn.Close()
		}
	}()
	t.client = pb.NewKubemqClient(t.conn)
	_, err = t.Ping(ctx)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Transport) setUnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if t.opts.AuthToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", t.opts.AuthToken)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (t *Transport) setStreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		if t.opts.AuthToken != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", t.opts.AuthToken)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func (t *Transport) Ping(ctx context.Context) (*ServerInfo, error) {
	res, err := t.client.Ping(ctx, &pb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("error connecting to kubemq server, %w", err)
	}
	si := &ServerInfo{
		Host:                res.Host,
		Version:             res.Version,
		ServerStartTime:     res.ServerStartTime,
		ServerUpTimeSeconds: res.ServerUpTimeSeconds,
	}
	return si, nil
}

func (t *Transport) KubeMQClient() pb.KubemqClient {
	return t.client
}
func (t *Transport) Close() error {
	err := t.conn.Close()
	if err != nil {
		return err
	}
	t.isClosed.Store(true)
	return nil
}
