package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fullTestLogger struct {
	testLogger
}

func (*fullTestLogger) Debug(string, ...any) {}

func startBufconnServer(t *testing.T) (*bufconn.Listener, *inlineServer) {
	t.Helper()
	impl := newInlineServer()
	lis := bufconn.Listen(testBufSize)
	srv := grpc.NewServer()
	pb.RegisterKubemqServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() { srv.GracefulStop() })
	return lis, impl
}

func newGRPCViaDialer(ctx context.Context, lis *bufconn.Listener, cfgOverrides ...func(*Config)) (*grpcTransport, error) {
	log := &testLogger{}
	sm := newStateMachine(StateCallbacks{}, log, nil)
	sm.transition(types.StateConnecting)

	cfg := Config{
		Host:              "bufconn",
		Port:              0,
		ClientID:          "test-client",
		ConnectionTimeout: 5 * time.Second,
		DrainTimeout:      2 * time.Second,
		MaxSendSize:       defaultMaxSendSize,
		MaxReceiveSize:    defaultMaxRcvSize,
		ReceiveBufferSize: 10,
	}
	for _, fn := range cfgOverrides {
		fn(&cfg)
	}

	ka := KeepaliveConfig{Time: 10 * time.Second, Timeout: 5 * time.Second}

	gt := &grpcTransport{
		cfg:               cfg,
		stateMachine:      sm,
		logger:            log,
		keepalive:         ka,
		drainTimeout:      cfg.DrainTimeout,
		subs:              &subscriptionTracker{},
		reconnectNotifyCh: make(chan struct{}),
		dispatcher:        newCallbackDispatcher(1, log),
		callbackTimeout:   30 * time.Second,
	}

	gt.reconnect = newReconnectLoop(
		types.ReconnectPolicy{},
		sm,
		log,
		gt.dial,
		func(ctx context.Context) {
			gt.notifyReconnected()
		},
	)

	tctx, cancel := context.WithCancel(context.Background())
	gt.cancelCtx = cancel

	dialOpts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(cfg.MaxReceiveSize),
			grpc.MaxCallSendMsgSize(cfg.MaxSendSize),
		),
		ka.dialOption(),
	}
	gt.dialOpts = dialOpts

	conn, err := grpc.NewClient("passthrough:///bufconn", dialOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	conn.Connect()
	connCtx, connCancel := context.WithTimeout(ctx, cfg.ConnectionTimeout)
	defer connCancel()

	for {
		s := conn.GetState()
		if s.String() == "READY" {
			break
		}
		if !conn.WaitForStateChange(connCtx, s) {
			_ = conn.Close()
			cancel()
			return nil, context.DeadlineExceeded
		}
	}

	gt.conn.Store(conn)
	gt.client.Store(pb.NewKubemqClient(conn))
	sm.transition(types.StateReady)

	go gt.monitorConnection(tctx)

	return gt, nil
}

func TestNewGRPC_FullLifecycle(t *testing.T) {
	lis, _ := startBufconnServer(t)

	ctx := context.Background()
	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	require.NotNil(t, gt)

	assert.Equal(t, types.StateReady, gt.State())

	info, err := gt.Ping(ctx)
	require.NoError(t, err)
	assert.Equal(t, "test-server", info.Host)

	err = gt.SendEvent(ctx, &SendEventRequest{
		ID: "ev1", Channel: "test", Body: []byte("hello"),
	})
	assert.NoError(t, err)

	err = gt.Close()
	assert.NoError(t, err)
	assert.Equal(t, types.StateClosed, gt.State())
}

func TestNewGRPC_ChannelManagement(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx := context.Background()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	err = gt.CreateChannel(ctx, &CreateChannelRequest{
		ClientID: "test", Channel: "my-ch", ChannelType: "events",
	})
	assert.NoError(t, err)

	err = gt.DeleteChannel(ctx, &DeleteChannelRequest{
		ClientID: "test", Channel: "my-ch", ChannelType: "events",
	})
	assert.NoError(t, err)

	_, err = gt.ListChannels(ctx, &ListChannelsRequest{
		ClientID: "test", ChannelType: "events",
	})
	assert.NoError(t, err)
}

func TestNewGRPC_FailedConnection(t *testing.T) {
	cfg := Config{
		Host:              "localhost",
		Port:              19999,
		ClientID:          "test-client",
		ConnectionTimeout: 500 * time.Millisecond,
		DrainTimeout:      1 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	gt, err := NewGRPC(ctx, cfg)
	assert.Error(t, err)
	assert.Nil(t, gt)
	assert.Contains(t, err.Error(), "initial connection failed")
}

func TestNewGRPC_Defaults(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx := context.Background()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	assert.Equal(t, types.StateReady, gt.State())
}

func TestNewGRPC_WithLogger(t *testing.T) {
	lis, _ := startBufconnServer(t)
	ctx := context.Background()

	gt, err := newGRPCViaDialer(ctx, lis, func(cfg *Config) {
		cfg.Logger = &fullTestLogger{}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = gt.Close() })
}

func TestNewGRPC_AllOperations(t *testing.T) {
	lis, impl := startBufconnServer(t)
	ctx := context.Background()

	gt, err := newGRPCViaDialer(ctx, lis)
	require.NoError(t, err)
	defer gt.Close()

	result, err := gt.SendEventStore(ctx, &SendEventStoreRequest{
		ID: "es1", Channel: "store", Body: []byte("data"),
	})
	require.NoError(t, err)
	assert.True(t, result.Sent)

	cmdResult, err := gt.SendCommand(ctx, &SendCommandRequest{
		ID: "cmd1", Channel: "cmd", Body: []byte("do"), Timeout: 5000,
	})
	require.NoError(t, err)
	assert.True(t, cmdResult.Executed)

	qResult, err := gt.SendQuery(ctx, &SendQueryRequest{
		ID: "q1", Channel: "q", Body: []byte("ask"), Timeout: 5000,
	})
	require.NoError(t, err)
	assert.True(t, qResult.Executed)

	err = gt.SendResponse(ctx, &SendResponseRequest{
		RequestID: "req1", ResponseTo: "resp",
	})
	assert.NoError(t, err)

	impl.mu.Lock()
	impl.ackAllQueueMessagesFn = func(_ context.Context, req *pb.AckAllQueueMessagesRequest) (*pb.AckAllQueueMessagesResponse, error) {
		return &pb.AckAllQueueMessagesResponse{RequestID: req.RequestID, AffectedMessages: 5}, nil
	}
	impl.mu.Unlock()

	ackResp, err := gt.AckAllQueueMessages(ctx, &AckAllQueueMessagesReq{
		Channel: "q-ch",
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(5), ackResp.AffectedMessages)
}
