package transport

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	pb "github.com/kubemq-io/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxSendSize = 1024 * 1024 * 100 // 100MB
	defaultMaxRcvSize  = 1024 * 1024 * 100 // 100MB

	defaultSendTimeout      = 5 * time.Second
	defaultRPCTimeout       = 10 * time.Second
	defaultQueueRecvTimeout = 10 * time.Second
)

func withDefaultTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); !ok {
		return context.WithTimeout(ctx, d)
	}
	return ctx, func() {}
}

// closeOnce is a sync.Once variant that returns whether the caller won the race.
type closeOnce struct {
	done atomic.Bool
}

func (co *closeOnce) Do() bool {
	return co.done.CompareAndSwap(false, true)
}

// inFlightTracker counts active operations for drain support.
type inFlightTracker struct {
	mu sync.RWMutex
	wg sync.WaitGroup
}

// grpcTransport implements the Transport interface using gRPC.
// A single Client instance uses one gRPC connection for all operations.
// Multiple concurrent operations are multiplexed over this single connection.
type grpcTransport struct {
	cfg          Config
	conn         atomic.Pointer[grpc.ClientConn]
	client       atomic.Value // stores pb.KubemqClient
	stateMachine *stateMachine
	reconnect    *reconnectLoop
	keepalive    KeepaliveConfig
	subs         *subscriptionTracker
	inFlight     inFlightTracker
	logger       logger
	authClose    func()
	dialOpts     []grpc.DialOption

	closed              atomic.Bool
	closeOnce           closeOnce
	cancelCtx           context.CancelFunc
	drainTimeout        time.Duration
	waitForReadyEnabled bool

	reconnectNotifyMu sync.Mutex
	reconnectNotifyCh chan struct{}

	dispatcher      *callbackDispatcher
	callbackTimeout time.Duration
}

// NewGRPC creates a new gRPC transport and establishes the initial connection.
// The connection is established before returning; use context for cancellation/timeout.
func NewGRPC(ctx context.Context, cfg Config) (*grpcTransport, error) {
	if cfg.MaxSendSize <= 0 {
		cfg.MaxSendSize = defaultMaxSendSize
	}
	if cfg.MaxReceiveSize <= 0 {
		cfg.MaxReceiveSize = defaultMaxRcvSize
	}
	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = 10 * time.Second
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 5 * time.Second
	}
	if cfg.MaxConcurrentCallbacks <= 0 {
		cfg.MaxConcurrentCallbacks = 1
	}
	if cfg.CallbackTimeout <= 0 {
		cfg.CallbackTimeout = 30 * time.Second
	}
	if cfg.ReceiveBufferSize <= 0 {
		cfg.ReceiveBufferSize = 10
	}

	var log logger = noopLogger{}
	if cfg.Logger != nil {
		log = cfg.Logger
	}

	ka := KeepaliveConfig{
		Time:                cfg.KeepaliveTime,
		Timeout:             cfg.KeepaliveTimeout,
		PermitWithoutStream: cfg.PermitKeepaliveWithoutStream,
	}
	if ka.Time <= 0 {
		ka.Time = 10 * time.Second
	}
	if ka.Timeout <= 0 {
		ka.Timeout = 5 * time.Second
	}

	callbacks := StateCallbacks{
		OnConnected:    cfg.OnConnected,
		OnDisconnected: cfg.OnDisconnected,
		OnReconnecting: cfg.OnReconnecting,
		OnReconnected:  cfg.OnReconnected,
		OnClosed:       cfg.OnClosed,
	}

	sm := newStateMachine(callbacks, log, cfg.OnBufferDrain)
	sm.transition(types.StateConnecting)

	t := &grpcTransport{
		cfg:                 cfg,
		keepalive:           ka,
		stateMachine:        sm,
		subs:                &subscriptionTracker{},
		logger:              log,
		drainTimeout:        cfg.DrainTimeout,
		waitForReadyEnabled: cfg.WaitForReady,
		reconnectNotifyCh:   make(chan struct{}),
		dispatcher:          newCallbackDispatcher(cfg.MaxConcurrentCallbacks, log),
		callbackTimeout:     cfg.CallbackTimeout,
	}

	t.reconnect = newReconnectLoop(
		cfg.ReconnectPolicy,
		sm,
		log,
		t.dial,
		func(ctx context.Context) {
			t.checkServerVersion(ctx)
			t.notifyReconnected()
		},
	)

	tctx, cancel := context.WithCancel(context.Background())
	t.cancelCtx = cancel

	t.dialOpts = t.buildDialOptions(tctx)

	connCtx, connCancel := context.WithTimeout(ctx, cfg.ConnectionTimeout)
	defer connCancel()

	if err := t.dial(connCtx); err != nil {
		sm.transition(types.StateClosed)
		if t.authClose != nil {
			t.authClose()
		}
		cancel()
		return nil, fmt.Errorf("initial connection failed: %w", err)
	}

	sm.transition(types.StateReady)

	go t.monitorConnection(tctx)
	go t.checkServerVersion(tctx)

	return t, nil
}

// State returns the current connection state. Thread-safe (atomic read).
func (t *grpcTransport) State() types.ConnectionState {
	return t.stateMachine.current()
}

// enterOperation marks an operation as in-flight. Returns false if client is closed.
func (t *grpcTransport) enterOperation() bool {
	t.inFlight.mu.RLock()
	defer t.inFlight.mu.RUnlock()
	if t.closed.Load() {
		return false
	}
	t.inFlight.wg.Add(1)
	return true
}

func (t *grpcTransport) exitOperation() {
	t.inFlight.wg.Done()
}

// waitForReady blocks until the connection enters READY state or the context expires.
func (t *grpcTransport) waitForReady(ctx context.Context) error {
	state := t.stateMachine.current()
	if state == types.StateReady {
		return nil
	}
	if state == types.StateClosed {
		return fmt.Errorf("kubemq: client closed")
	}

	ch := t.stateMachine.waitForState(types.StateReady)
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("timed out waiting for connection to become ready: %w", ctx.Err())
	}
}

// checkReady verifies the transport is ready for operations.
// If waitForReadyEnabled, blocks until ready; otherwise fails immediately.
func (t *grpcTransport) checkReady(ctx context.Context) error {
	if t.waitForReadyEnabled {
		return t.waitForReady(ctx)
	}
	if t.stateMachine.current() != types.StateReady {
		return fmt.Errorf("connection not ready (state=%s)", t.stateMachine.current().String())
	}
	return nil
}

// Close initiates graceful shutdown.
// Idempotent: safe to call multiple times.
func (t *grpcTransport) Close() error {
	if !t.closeOnce.Do() {
		return nil
	}

	currentState := t.stateMachine.current()

	if currentState == types.StateReconnecting {
		t.reconnect.cancel()
		t.reconnect.discardBuffer()
		t.dispatcher.drain(t.callbackTimeout)
		t.stateMachine.transition(types.StateClosed)
		if t.authClose != nil {
			t.authClose()
		}
		t.cancelCtx()
		return nil
	}

	t.closed.Store(true)
	t.inFlight.mu.Lock()
	t.inFlight.mu.Unlock()

	t.reconnect.flushBuffer()

	t.drainInFlight()

	var err error
	if conn := t.conn.Load(); conn != nil {
		err = conn.Close()
	}

	// Phase 3: Callback drain (10-concurrency-spec.md, REQ-CONC-5)
	// Runs AFTER conn.Close() — safe because subscription callbacks only
	// process received messages and do not make outbound RPC calls.
	if drained := t.dispatcher.drain(t.callbackTimeout); !drained {
		t.logger.Warn("callback drain timeout exceeded during Close",
			"timeout", t.callbackTimeout,
		)
	}

	t.stateMachine.transition(types.StateClosed)

	if t.authClose != nil {
		t.authClose()
	}
	t.cancelCtx()

	return err
}

func (t *grpcTransport) drainInFlight() {
	done := make(chan struct{})
	go func() {
		t.inFlight.wg.Wait()
		close(done)
	}()

	// GO-57: use time.NewTimer instead of time.After
	timer := time.NewTimer(t.drainTimeout)
	defer timer.Stop()

	select {
	case <-done:
		t.logger.Info("all in-flight operations drained")
	case <-timer.C:
		t.logger.Warn("drain timeout exceeded, closing with in-flight operations",
			"timeout", t.drainTimeout,
		)
	}
}

// monitorConnection watches the gRPC connection state and triggers
// reconnection when the connection is lost.
func (t *grpcTransport) monitorConnection(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn := t.conn.Load()
		if conn == nil {
			return
		}

		currentGRPCState := conn.GetState()
		if !conn.WaitForStateChange(ctx, currentGRPCState) {
			return
		}

		newGRPCState := conn.GetState()
		if newGRPCState == connectivity.TransientFailure || newGRPCState == connectivity.Shutdown {
			if t.stateMachine.current() == types.StateReady {
				t.logger.Warn("connection lost, starting reconnection",
					"grpc_state", newGRPCState.String(),
				)
				t.stateMachine.transition(types.StateReconnecting)
				t.reconnect.start(ctx)
			}
		}
	}
}

// notifyReconnected broadcasts to all waiting subscribe loops that a
// reconnection has succeeded, by closing the current channel and swapping
// in a fresh one.
func (t *grpcTransport) notifyReconnected() {
	t.reconnectNotifyMu.Lock()
	old := t.reconnectNotifyCh
	t.reconnectNotifyCh = make(chan struct{})
	t.reconnectNotifyMu.Unlock()
	close(old)
}

// waitReconnect returns the current reconnect notification channel.
// Subscribe loops select on this channel; it is closed when reconnection succeeds.
func (t *grpcTransport) waitReconnect() <-chan struct{} {
	t.reconnectNotifyMu.Lock()
	defer t.reconnectNotifyMu.Unlock()
	return t.reconnectNotifyCh
}

// dial creates a new gRPC connection. Each call creates a fresh grpc.ClientConn,
// which forces DNS re-resolution.
func (t *grpcTransport) dial(ctx context.Context) error {
	address := fmt.Sprintf("%s:%d", t.cfg.Host, t.cfg.Port)
	conn, err := grpc.NewClient(address, t.dialOpts...)
	if err != nil {
		return fmt.Errorf("gRPC dial failed: %w", err)
	}

	conn.Connect()
	connCtx, cancel := context.WithTimeout(ctx, t.cfg.ConnectionTimeout)
	defer cancel()

	for {
		s := conn.GetState()
		if s == connectivity.Ready {
			break
		}
		if s == connectivity.TransientFailure || s == connectivity.Shutdown {
			_ = conn.Close()
			return fmt.Errorf("connection failed with state: %s", s)
		}
		if !conn.WaitForStateChange(connCtx, s) {
			_ = conn.Close()
			return fmt.Errorf("connection timeout after %s", t.cfg.ConnectionTimeout)
		}
	}

	if old := t.conn.Swap(conn); old != nil {
		_ = old.Close()
	}

	client := pb.NewKubemqClient(conn)
	t.client.Store(client)

	return nil
}

func (t *grpcTransport) buildDialOptions(clientCtx context.Context) []grpc.DialOption {
	connOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(t.cfg.MaxReceiveSize),
			grpc.MaxCallSendMsgSize(t.cfg.MaxSendSize),
		),
		t.keepalive.dialOption(),
	}

	if t.cfg.TLSConfig != nil || t.cfg.InsecureSkipVerify {
		tlsCfg, err := buildTLSConfigWithLogger(t.cfg.TLSConfig, t.cfg.InsecureSkipVerify, t.logger)
		if err != nil {
			t.logger.Error("TLS setup failed, falling back to insecure", "error", err)
			connOptions = append(connOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			connOptions = append(connOptions, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
		}
	} else if t.cfg.IsSecured {
		tlsCreds, err := t.buildLegacyTLSCredentials()
		if err != nil {
			t.logger.Error("TLS setup failed, falling back to insecure", "error", err)
			connOptions = append(connOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
		} else {
			connOptions = append(connOptions, grpc.WithTransportCredentials(tlsCreds))
		}
	} else {
		connOptions = append(connOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	t.logSecurityWarnings()

	errmapInt := middleware.NewErrmapInterceptor()
	retryInt := middleware.NewRetryInterceptor(t.cfg.RetryPolicy, t.cfg.Logger, t.cfg.MaxConcurrentRetries)

	unaryInterceptors := []grpc.UnaryClientInterceptor{
		errmapInt.UnaryInterceptor(),
		retryInt.UnaryInterceptor(),
	}
	streamInterceptors := []grpc.StreamClientInterceptor{
		errmapInt.StreamInterceptor(),
	}

	if t.cfg.CredentialProvider != nil {
		authInt := middleware.NewAuthInterceptor(t.cfg.CredentialProvider, t.cfg.Logger, clientCtx)
		t.authClose = authInt.Close
		unaryInterceptors = append([]grpc.UnaryClientInterceptor{authInt.UnaryInterceptor()}, unaryInterceptors...)
		streamInterceptors = append([]grpc.StreamClientInterceptor{authInt.StreamInterceptor()}, streamInterceptors...)
	} else if t.cfg.AuthToken != "" {
		provider := types.NewStaticTokenProvider(t.cfg.AuthToken)
		authInt := middleware.NewAuthInterceptor(provider, t.cfg.Logger, clientCtx)
		t.authClose = authInt.Close
		unaryInterceptors = append([]grpc.UnaryClientInterceptor{authInt.UnaryInterceptor()}, unaryInterceptors...)
		streamInterceptors = append([]grpc.StreamClientInterceptor{authInt.StreamInterceptor()}, streamInterceptors...)
	}

	connOptions = append(connOptions,
		grpc.WithChainUnaryInterceptor(unaryInterceptors...),
		grpc.WithChainStreamInterceptor(streamInterceptors...),
	)

	return connOptions
}

// buildLegacyTLSCredentials handles the v1 IsSecured/CertFile/CertData fields
// for backward compatibility during transition.
func (t *grpcTransport) buildLegacyTLSCredentials() (credentials.TransportCredentials, error) {
	if t.cfg.CertFile != "" {
		return credentials.NewClientTLSFromFile(t.cfg.CertFile, t.cfg.ServerOverrideDomain)
	}
	if t.cfg.CertData != "" {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM([]byte(t.cfg.CertData)) {
			return nil, fmt.Errorf("failed to append certificates to pool")
		}
		return credentials.NewClientTLSFromCert(certPool, t.cfg.ServerOverrideDomain), nil
	}
	return nil, fmt.Errorf("no valid TLS certificate provided")
}

func (t *grpcTransport) logSecurityWarnings() {
	if t.cfg.InsecureSkipVerify {
		t.logger.Warn("certificate verification is disabled — connections are vulnerable to man-in-the-middle attacks")
	}
	if t.cfg.TLSConfig == nil && !t.cfg.InsecureSkipVerify && !t.cfg.IsSecured {
		t.logger.Info("connecting without TLS encryption")
	}
}

// getClient returns the current gRPC stub client.
func (t *grpcTransport) getClient() pb.KubemqClient {
	c, _ := t.client.Load().(pb.KubemqClient)
	return c
}

// Ping sends a ping to the server and returns server info.
func (t *grpcTransport) Ping(ctx context.Context) (*ServerInfoResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	result, err := t.getClient().Ping(ctx, &pb.Empty{})
	if err != nil {
		return nil, fmt.Errorf("ping failed: %w", err)
	}
	return ServerInfoFromProto(result), nil
}

// SendEvent sends a fire-and-forget event.
func (t *grpcTransport) SendEvent(ctx context.Context, req *SendEventRequest) error {
	if !t.enterOperation() {
		return fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultSendTimeout)
	defer cancel()
	pbEvent := EventToProto(req, t.cfg.ClientID)
	result, err := t.getClient().SendEvent(ctx, pbEvent)
	if err != nil {
		return fmt.Errorf("send event failed: %w", err)
	}
	if result != nil && !result.Sent {
		return fmt.Errorf("event not sent: %s", result.Error)
	}
	return nil
}

// SendEventStore sends an event to the event store.
func (t *grpcTransport) SendEventStore(ctx context.Context, req *SendEventStoreRequest) (*SendEventStoreResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultSendTimeout)
	defer cancel()
	pbEvent := EventStoreToProto(req, t.cfg.ClientID)
	result, err := t.getClient().SendEvent(ctx, pbEvent)
	if err != nil {
		return nil, fmt.Errorf("send event store failed: %w", err)
	}
	return EventStoreResultFromProto(result), nil
}

// SendEventsStream opens a bidirectional stream for sending events (both regular and store).
func (t *grpcTransport) SendEventsStream(ctx context.Context) (*EventStreamHandle, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := t.getClient().SendEventsStream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("send events stream: %w", err)
	}

	doneCh := make(chan struct{})
	resultCh := make(chan *EventStreamResult, 16)
	go func() {
		defer close(doneCh)
		defer close(resultCh)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			result := EventStreamResultFromProto(resp)
			if result != nil {
				select {
				case resultCh <- result:
				case <-streamCtx.Done():
					return
				}
			}
		}
	}()

	handle := &EventStreamHandle{
		Results: resultCh,
		Done:    doneCh,
		SendFn: func(item *EventStreamItem) error {
			pbEvent := EventStreamItemToProto(item, t.cfg.ClientID)
			return stream.Send(pbEvent)
		},
		closeFn: func() {
			_ = stream.CloseSend()
			streamCancel()
		},
	}
	return handle, nil
}

// SendCommand sends a command and waits for a response.
func (t *grpcTransport) SendCommand(ctx context.Context, req *SendCommandRequest) (*SendCommandResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbReq := CommandToProto(req, t.cfg.ClientID)
	result, err := t.getClient().SendRequest(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("send command failed: %w", err)
	}
	return CommandResultFromProto(result), nil
}

// SendQuery sends a query and waits for a response.
func (t *grpcTransport) SendQuery(ctx context.Context, req *SendQueryRequest) (*SendQueryResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbReq := QueryToProto(req, t.cfg.ClientID)
	result, err := t.getClient().SendRequest(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("send query failed: %w", err)
	}
	return QueryResultFromProto(result), nil
}

// SendResponse sends a response to a command or query.
func (t *grpcTransport) SendResponse(ctx context.Context, req *SendResponseRequest) error {
	if !t.enterOperation() {
		return fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbResp := ResponseToProto(req)
	_, err := t.getClient().SendResponse(ctx, pbResp)
	if err != nil {
		return fmt.Errorf("send response failed: %w", err)
	}
	return nil
}

// SendQueueMessages sends one or more queue messages.
func (t *grpcTransport) SendQueueMessages(ctx context.Context, req *SendQueueMessagesRequest) (*SendQueueMessagesResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultSendTimeout)
	defer cancel()
	pbBatch := &pb.QueueMessagesBatchRequest{
		BatchID:  "",
		Messages: make([]*pb.QueueMessage, 0, len(req.Messages)),
	}
	for _, m := range req.Messages {
		pbBatch.Messages = append(pbBatch.Messages, QueueMessageToProto(m, t.cfg.ClientID))
	}
	result, err := t.getClient().SendQueueMessagesBatch(ctx, pbBatch)
	if err != nil {
		return nil, fmt.Errorf("send queue messages failed: %w", err)
	}
	out := &SendQueueMessagesResult{
		Results: make([]*SendQueueMessageResultItem, 0, len(result.Results)),
	}
	for _, r := range result.Results {
		out.Results = append(out.Results, SendQueueMessageResultFromProto(r))
	}
	return out, nil
}

// ReceiveQueueMessages receives messages from a queue.
func (t *grpcTransport) ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesReq) (*ReceiveQueueMessagesResp, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultQueueRecvTimeout)
	defer cancel()
	pbReq := &pb.ReceiveQueueMessagesRequest{
		RequestID:           req.RequestID,
		ClientID:            firstNonEmpty(req.ClientID, t.cfg.ClientID),
		Channel:             req.Channel,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		IsPeak:              req.IsPeak,
	}
	result, err := t.getClient().ReceiveQueueMessages(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("receive queue messages failed: %w", err)
	}
	return ReceiveQueueMessagesRespFromProto(result), nil
}

// AckAllQueueMessages acknowledges all messages in a queue.
func (t *grpcTransport) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesReq) (*AckAllQueueMessagesResp, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	pbReq := &pb.AckAllQueueMessagesRequest{
		RequestID:       req.RequestID,
		ClientID:        firstNonEmpty(req.ClientID, t.cfg.ClientID),
		Channel:         req.Channel,
		WaitTimeSeconds: req.WaitTimeSeconds,
	}
	result, err := t.getClient().AckAllQueueMessages(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("ack all queue messages failed: %w", err)
	}
	return AckAllRespFromProto(result), nil
}

// QueuesInfo queries queue info from the server.
func (t *grpcTransport) QueuesInfo(ctx context.Context, filter string) (*QueuesInfoResult, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	result, err := t.getClient().QueuesInfo(ctx, &pb.QueuesInfoRequest{
		RequestID: "",
		QueueName: filter,
	})
	if err != nil {
		return nil, fmt.Errorf("queues info failed: %w", err)
	}
	if result == nil || result.Info == nil {
		return &QueuesInfoResult{}, nil
	}
	out := &QueuesInfoResult{
		TotalQueue: result.Info.TotalQueue,
		Sent:       result.Info.Sent,
		Delivered:  result.Info.Delivered,
		Waiting:    result.Info.Waiting,
	}
	for _, q := range result.Info.Queues {
		out.Queues = append(out.Queues, &QueueInfoItem{
			Name:        q.Name,
			Messages:    q.Messages,
			Bytes:       q.Bytes,
			FirstSeq:    q.FirstSequence,
			LastSeq:     q.LastSequence,
			Sent:        q.Sent,
			Delivered:   q.Delivered,
			Waiting:     q.Waiting,
			Subscribers: q.Subscribers,
		})
	}
	return out, nil
}

const requestChannel = "kubemq.cluster.internal.requests"

// SubscribeToEvents opens a server-streaming subscription for events.
func (t *grpcTransport) SubscribeToEvents(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error) {
	return t.subscribe(ctx, req, pb.Subscribe_Events)
}

// SubscribeToEventsStore opens a server-streaming subscription for stored events.
func (t *grpcTransport) SubscribeToEventsStore(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error) {
	return t.subscribe(ctx, req, pb.Subscribe_EventsStore)
}

// SubscribeToCommands opens a server-streaming subscription for commands.
func (t *grpcTransport) SubscribeToCommands(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error) {
	return t.subscribe(ctx, req, pb.Subscribe_Commands)
}

// SubscribeToQueries opens a server-streaming subscription for queries.
func (t *grpcTransport) SubscribeToQueries(ctx context.Context, req *SubscribeRequest) (*StreamHandle, error) {
	return t.subscribe(ctx, req, pb.Subscribe_Queries)
}

func (t *grpcTransport) subscribe(ctx context.Context, req *SubscribeRequest, subType pb.Subscribe_SubscribeType) (*StreamHandle, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	subID := fmt.Sprintf("%s:%s:%d", req.Channel, req.Group, subType)
	patternName := subscribePatternName(subType)

	msgCh := make(chan any, t.cfg.ReceiveBufferSize)
	errCh := make(chan error, 8)
	subCtx, subCancel := context.WithCancel(ctx)

	t.subs.register(&subscriptionRecord{
		id:      subID,
		pattern: patternName,
		channel: req.Channel,
		group:   req.Group,
	})

	buildPbSub := func() *pb.Subscribe {
		s := &pb.Subscribe{
			SubscribeTypeData:    subType,
			ClientID:             firstNonEmpty(req.ClientID, t.cfg.ClientID),
			Channel:              req.Channel,
			Group:                req.Group,
			EventsStoreTypeData:  pb.Subscribe_EventsStoreType(req.SubscriptionType),
			EventsStoreTypeValue: req.SubscriptionValue,
		}
		if subType == pb.Subscribe_EventsStore {
			if lastSeq := t.subs.getLastSeq(subID); lastSeq > 0 {
				s.EventsStoreTypeData = pb.Subscribe_EventsStoreType(4) // StartAtSequence
				s.EventsStoreTypeValue = lastSeq + 1
			}
		}
		return s
	}

	openStream := func(sctx context.Context) (recvStream, error) {
		pbSub := buildPbSub()
		switch subType {
		case pb.Subscribe_Events, pb.Subscribe_EventsStore:
			s, err := t.getClient().SubscribeToEvents(sctx, pbSub)
			if err != nil {
				return nil, fmt.Errorf("subscribe to events stream: %w", err)
			}
			return &eventStreamAdapter{s}, nil
		case pb.Subscribe_Commands, pb.Subscribe_Queries:
			s, err := t.getClient().SubscribeToRequests(sctx, pbSub)
			if err != nil {
				return nil, fmt.Errorf("subscribe to requests stream: %w", err)
			}
			return &requestStreamAdapter{s}, nil
		default:
			return nil, fmt.Errorf("unknown subscribe type: %v", subType)
		}
	}

	stream, err := openStream(subCtx)
	if err != nil {
		subCancel()
		t.subs.remove(subID)
		return nil, err
	}

	go t.subscribeLoop(subCtx, subID, subType, stream, openStream, msgCh, errCh)

	return NewStreamHandle(msgCh, errCh, func() {
		subCancel()
		t.subs.remove(subID)
	}), nil
}

// subscribeLoop runs the recv loop for a subscription and handles reconnection.
// It keeps msgCh/errCh open across reconnections and only closes them when the
// subscription context is cancelled or max reconnect attempts are exhausted.
func (t *grpcTransport) subscribeLoop(
	ctx context.Context,
	subID string,
	subType pb.Subscribe_SubscribeType,
	stream recvStream,
	openStream func(context.Context) (recvStream, error),
	msgCh chan<- any,
	errCh chan<- error,
) {
	defer close(msgCh)
	defer close(errCh)

	for {
		recvErr := t.recvLoop(ctx, stream, subType, subID, msgCh)
		if recvErr == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		if !isConnectionError(recvErr) {
			select {
			case errCh <- recvErr:
			case <-ctx.Done():
			}
			return
		}

		t.logger.Info("subscription stream lost, waiting for reconnection",
			"sub_id", subID,
		)

		reconnectCh := t.waitReconnect()
		select {
		case <-ctx.Done():
			return
		case <-reconnectCh:
		}

		t.logger.Info("reconnection detected, re-opening subscription stream",
			"sub_id", subID,
		)

		var err error
		stream, err = openStream(ctx)
		if err != nil {
			t.logger.Error("subscription re-open failed after reconnect",
				"sub_id", subID,
				"error", err,
			)
			select {
			case errCh <- fmt.Errorf("subscription recovery failed: %w", err):
			case <-ctx.Done():
			}
			return
		}
	}
}

func subscribePatternName(subType pb.Subscribe_SubscribeType) string {
	switch subType {
	case pb.Subscribe_Events:
		return "events"
	case pb.Subscribe_EventsStore:
		return "events_store"
	case pb.Subscribe_Commands:
		return "commands"
	case pb.Subscribe_Queries:
		return "queries"
	default:
		return "unknown"
	}
}

type eventStreamAdapter struct {
	stream pb.Kubemq_SubscribeToEventsClient
}

func (a *eventStreamAdapter) Recv() (interface{}, error) {
	return a.stream.Recv()
}

type requestStreamAdapter struct {
	stream pb.Kubemq_SubscribeToRequestsClient
}

func (a *requestStreamAdapter) Recv() (interface{}, error) {
	return a.stream.Recv()
}

type recvStream interface {
	Recv() (interface{}, error)
}

// recvLoop reads from the stream and forwards converted messages to msgCh.
// It returns nil on clean context cancellation, or the recv error on stream failure.
// Channels are NOT closed here — the caller (subscribeLoop) owns channel lifecycle.
func (t *grpcTransport) recvLoop(ctx context.Context, stream recvStream, subType pb.Subscribe_SubscribeType, subID string, msgCh chan<- any) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		raw, err := stream.Recv()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return err
			}
		}

		var msg any
		switch subType {
		case pb.Subscribe_Events:
			if e, ok := raw.(*pb.EventReceive); ok {
				msg = EventReceiveFromProto(e)
			}
		case pb.Subscribe_EventsStore:
			if e, ok := raw.(*pb.EventReceive); ok {
				item := EventStoreReceiveFromProto(e)
				msg = item
				if item.Sequence > 0 {
					t.subs.updateLastSeq(subID, int64(item.Sequence))
				}
			}
		case pb.Subscribe_Commands:
			if r, ok := raw.(*pb.Request); ok {
				msg = CommandReceiveFromProto(r)
			}
		case pb.Subscribe_Queries:
			if r, ok := raw.(*pb.Request); ok {
				msg = QueryReceiveFromProto(r)
			}
		}

		if msg != nil {
			select {
			case <-ctx.Done():
				return nil
			case msgCh <- msg:
			}
		}
	}
}

// QueueUpstream opens a bidirectional stream for sending queue messages.
func (t *grpcTransport) QueueUpstream(ctx context.Context) (*QueueUpstreamHandle, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := t.getClient().QueuesUpstream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("queue upstream: %w", err)
	}

	doneCh := make(chan struct{})
	resultCh := make(chan *QueueUpstreamResult, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(doneCh)
		defer close(resultCh)
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			result := QueueUpstreamResponseFromProto(resp)
			select {
			case resultCh <- result:
			case <-streamCtx.Done():
				return
			}
		}
	}()

	handle := &QueueUpstreamHandle{
		Done:    doneCh,
		Results: resultCh,
		Errors:  errCh,
		SendFn: func(requestID string, msgs []*QueueMessageItem) error {
			pbMsgs := make([]*pb.QueueMessage, 0, len(msgs))
			for _, m := range msgs {
				pbMsgs = append(pbMsgs, QueueMessageToProto(m, t.cfg.ClientID))
			}
			return stream.Send(&pb.QueuesUpstreamRequest{
				RequestID: requestID,
				Messages:  pbMsgs,
			})
		},
		closeFn: func() {
			_ = stream.CloseSend()
			streamCancel()
		},
	}
	return handle, nil
}

// QueueDownstream opens a bidirectional stream for receiving and managing queue messages.
func (t *grpcTransport) QueueDownstream(ctx context.Context, req *QueueDownstreamRequest) (*QueueDownstreamHandle, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := t.getClient().QueuesDownstream(streamCtx)
	if err != nil {
		streamCancel()
		return nil, fmt.Errorf("queue downstream: %w", err)
	}

	msgCh := make(chan any, t.cfg.ReceiveBufferSize)
	errCh := make(chan error, 8)

	go func() {
		defer close(msgCh)
		defer close(errCh)
		for {
			resp, err := stream.Recv()
			if err != nil {
				select {
				case <-streamCtx.Done():
				case errCh <- err:
				}
				return
			}
			if pb.QueuesDownstreamRequestType(resp.RequestTypeData) == 11 {
				select {
				case <-streamCtx.Done():
				case errCh <- fmt.Errorf("kubemq: server closed downstream stream"):
				}
				return
			}
			if resp.IsError {
				select {
				case <-streamCtx.Done():
					return
				case errCh <- fmt.Errorf("queue downstream error: %s", resp.Error):
				}
				continue
			}
			result := QueueDownstreamResponseFromProto(resp)
			select {
			case <-streamCtx.Done():
				return
			case msgCh <- result:
			}
		}
	}()

	handle := &QueueDownstreamHandle{
		Messages: msgCh,
		Errors:   errCh,
		SendFn: func(r *QueueDownstreamSendRequest) error {
			pbReq := &pb.QueuesDownstreamRequest{
				RequestID:        r.RequestID,
				ClientID:         firstNonEmpty(r.ClientID, t.cfg.ClientID),
				RequestTypeData:  pb.QueuesDownstreamRequestType(r.RequestType),
				Channel:          r.Channel,
				MaxItems:         r.MaxItems,
				WaitTimeout:      r.WaitTimeout,
				AutoAck:          r.AutoAck,
				ReQueueChannel:   r.ReQueueChannel,
				SequenceRange:    r.SequenceRange,
				RefTransactionId: r.RefTransactionID,
				Metadata:         r.Metadata,
			}
			return stream.Send(pbReq)
		},
		closeFn: func() {
			_ = stream.CloseSend()
			streamCancel()
		},
	}
	return handle, nil
}

// CreateChannel creates a channel by sending an internal query to the KubeMQ server.
func (t *grpcTransport) CreateChannel(ctx context.Context, req *CreateChannelRequest) error {
	if !t.enterOperation() {
		return fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbReq := &pb.Request{
		RequestID:       "",
		RequestTypeData: pb.Request_Query,
		ClientID:        firstNonEmpty(req.ClientID, t.cfg.ClientID),
		Channel:         requestChannel,
		Metadata:        "create-channel",
		Timeout:         int32(defaultRPCTimeout.Milliseconds()),
		Tags: map[string]string{
			"channel_type": req.ChannelType,
			"channel":      req.Channel,
			"client_id":    firstNonEmpty(req.ClientID, t.cfg.ClientID),
		},
	}
	resp, err := t.getClient().SendRequest(ctx, pbReq)
	if err != nil {
		return fmt.Errorf("create channel failed: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("create channel: %s", resp.Error)
	}
	return nil
}

// DeleteChannel deletes a channel by sending an internal query to the KubeMQ server.
func (t *grpcTransport) DeleteChannel(ctx context.Context, req *DeleteChannelRequest) error {
	if !t.enterOperation() {
		return fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbReq := &pb.Request{
		RequestID:       "",
		RequestTypeData: pb.Request_Query,
		ClientID:        firstNonEmpty(req.ClientID, t.cfg.ClientID),
		Channel:         requestChannel,
		Metadata:        "delete-channel",
		Timeout:         int32(defaultRPCTimeout.Milliseconds()),
		Tags: map[string]string{
			"channel_type": req.ChannelType,
			"channel":      req.Channel,
			"client_id":    firstNonEmpty(req.ClientID, t.cfg.ClientID),
		},
	}
	resp, err := t.getClient().SendRequest(ctx, pbReq)
	if err != nil {
		return fmt.Errorf("delete channel failed: %w", err)
	}
	if resp.Error != "" {
		return fmt.Errorf("delete channel: %s", resp.Error)
	}
	return nil
}

// ListChannels lists channels by sending an internal query to the KubeMQ server.
func (t *grpcTransport) ListChannels(ctx context.Context, req *ListChannelsRequest) ([]*ChannelInfo, error) {
	if !t.enterOperation() {
		return nil, fmt.Errorf("kubemq: client closed")
	}
	defer t.exitOperation()

	if err := t.checkReady(ctx); err != nil {
		return nil, err
	}

	ctx, cancel := withDefaultTimeout(ctx, defaultRPCTimeout)
	defer cancel()
	pbReq := &pb.Request{
		RequestID:       "",
		RequestTypeData: pb.Request_Query,
		ClientID:        firstNonEmpty(req.ClientID, t.cfg.ClientID),
		Channel:         requestChannel,
		Metadata:        "list-channels",
		Timeout:         int32(defaultRPCTimeout.Milliseconds()),
		Tags: map[string]string{
			"channel_type": req.ChannelType,
			"client_id":    firstNonEmpty(req.ClientID, t.cfg.ClientID),
		},
	}
	if req.Search != "" {
		pbReq.Tags["search"] = req.Search
	}
	resp, err := t.getClient().SendRequest(ctx, pbReq)
	if err != nil {
		return nil, fmt.Errorf("list channels failed: %w", err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("list channels: %s", resp.Error)
	}
	channels, err := parseChannelList(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("list channels: failed to parse response: %w", err)
	}
	return channels, nil
}

type channelListItem struct {
	Name         string       `json:"name"`
	Type         string       `json:"type"`
	LastActivity int64        `json:"lastActivity"`
	IsActive     bool         `json:"isActive"`
	Incoming     *channelStat `json:"incoming"`
	Outgoing     *channelStat `json:"outgoing"`
}

type channelStat struct {
	Messages  int64 `json:"messages"`
	Volume    int64 `json:"volume"`
	Responses int64 `json:"responses"`
	Waiting   int64 `json:"waiting"`
	Expired   int64 `json:"expired"`
	Delayed   int64 `json:"delayed"`
}

func parseChannelList(data []byte) ([]*ChannelInfo, error) {
	if len(data) == 0 {
		return nil, nil
	}
	var items []channelListItem
	if err := json.Unmarshal(data, &items); err != nil {
		return nil, fmt.Errorf("invalid channel list JSON: %w", err)
	}
	out := make([]*ChannelInfo, 0, len(items))
	for _, item := range items {
		ci := &ChannelInfo{
			Name:         item.Name,
			Type:         item.Type,
			LastActivity: item.LastActivity,
			IsActive:     item.IsActive,
		}
		if item.Incoming != nil {
			ci.Incoming = &ChannelStats{
				Messages:  item.Incoming.Messages,
				Volume:    item.Incoming.Volume,
				Responses: item.Incoming.Responses,
				Waiting:   item.Incoming.Waiting,
				Expired:   item.Incoming.Expired,
				Delayed:   item.Incoming.Delayed,
			}
		}
		if item.Outgoing != nil {
			ci.Outgoing = &ChannelStats{
				Messages:  item.Outgoing.Messages,
				Volume:    item.Outgoing.Volume,
				Responses: item.Outgoing.Responses,
				Waiting:   item.Outgoing.Waiting,
				Expired:   item.Outgoing.Expired,
				Delayed:   item.Outgoing.Delayed,
			}
		}
		out = append(out, ci)
	}
	return out, nil
}
