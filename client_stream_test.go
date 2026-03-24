package kubemq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/middleware"
	"github.com/kubemq-io/kubemq-go/v2/internal/testutil"
	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	pb "github.com/kubemq-io/protobuf/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func newStreamTestClient(t *testing.T) (*Client, *testutil.MockTransport) {
	t.Helper()
	mt := testutil.NewMockTransport()
	otel := middleware.NewOTelInterceptor(nil, nil, nil, "test", 0,
		middleware.CardinalityConfig{Threshold: 100}, "test")
	c := &Client{
		opts:      GetDefaultOptions(),
		transport: mt,
		otel:      otel,
	}
	return c, mt
}

// ---------------------------------------------------------------------------
// SendEventStream
// ---------------------------------------------------------------------------

func TestClient_SendEventStream_Success(t *testing.T) {
	resultCh := make(chan *transport.EventStreamResult, 16)
	doneCh := make(chan struct{})

	c, mt := newStreamTestClient(t)
	mt.OnSendEventsStream(func(_ context.Context) (*transport.EventStreamHandle, error) {
		return &transport.EventStreamHandle{
			Results: resultCh,
			Done:    doneCh,
			SendFn:  func(_ *transport.EventStreamItem) error { return nil },
		}, nil
	})

	handle, err := c.SendEventStream(context.Background())
	require.NoError(t, err)
	require.NotNil(t, handle)

	err = handle.Send(NewEvent().SetChannel("test-ch").SetBody([]byte("hi")))
	assert.NoError(t, err)

	close(resultCh)

	select {
	case _, ok := <-handle.Errors:
		if ok {
			t.Fatal("expected errors channel to drain cleanly")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for errors channel to close")
	}
}

func TestClient_SendEventStream_ClosedClient(t *testing.T) {
	c, _ := newStreamTestClient(t)
	_ = c.Close()

	handle, err := c.SendEventStream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_SendEventStream_TransportError(t *testing.T) {
	c, mt := newStreamTestClient(t)
	mt.OnSendEventsStream(func(_ context.Context) (*transport.EventStreamHandle, error) {
		return nil, errors.New("stream unavailable")
	})

	handle, err := c.SendEventStream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "stream unavailable")
}

func TestClient_SendEventStream_ErrorPropagation(t *testing.T) {
	resultCh := make(chan *transport.EventStreamResult, 1)
	doneCh := make(chan struct{})

	c, mt := newStreamTestClient(t)
	mt.OnSendEventsStream(func(_ context.Context) (*transport.EventStreamHandle, error) {
		return &transport.EventStreamHandle{
			Results: resultCh,
			Done:    doneCh,
			SendFn:  func(_ *transport.EventStreamItem) error { return nil },
		}, nil
	})

	handle, err := c.SendEventStream(context.Background())
	require.NoError(t, err)

	resultCh <- &transport.EventStreamResult{EventID: "ev-1", Sent: false, Error: "fail msg"}
	close(resultCh)

	select {
	case e := <-handle.Errors:
		require.Error(t, e)
		assert.Contains(t, e.Error(), "fail msg")
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for error")
	}
}

// ---------------------------------------------------------------------------
// SendEventStoreStream
// ---------------------------------------------------------------------------

func TestClient_SendEventStoreStream_Success(t *testing.T) {
	resultCh := make(chan *transport.EventStreamResult, 1)
	doneCh := make(chan struct{})

	c, mt := newStreamTestClient(t)
	mt.OnSendEventsStream(func(_ context.Context) (*transport.EventStreamHandle, error) {
		return &transport.EventStreamHandle{
			Results: resultCh,
			Done:    doneCh,
			SendFn: func(item *transport.EventStreamItem) error {
				assert.True(t, item.Store)
				resultCh <- &transport.EventStreamResult{EventID: item.ID, Sent: true}
				return nil
			},
		}, nil
	})

	handle, err := c.SendEventStoreStream(context.Background())
	require.NoError(t, err)

	es := NewEventStore().SetId("es-1").SetChannel("store-ch").SetBody([]byte("data"))
	err = handle.Send(es)
	require.NoError(t, err)

	select {
	case r := <-handle.Results:
		require.NotNil(t, r)
		assert.Equal(t, "es-1", r.EventID)
		assert.True(t, r.Sent)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	close(resultCh)
	handle.Close()
}

func TestClient_SendEventStoreStream_ClosedClient(t *testing.T) {
	c, _ := newStreamTestClient(t)
	_ = c.Close()

	handle, err := c.SendEventStoreStream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_SendEventStoreStream_TransportError(t *testing.T) {
	c, mt := newStreamTestClient(t)
	mt.OnSendEventsStream(func(_ context.Context) (*transport.EventStreamHandle, error) {
		return nil, errors.New("stream open failed")
	})

	handle, err := c.SendEventStoreStream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "stream open failed")
}

// ---------------------------------------------------------------------------
// QueueUpstream
// ---------------------------------------------------------------------------

func TestClient_QueueUpstream_Success(t *testing.T) {
	resultCh := make(chan *transport.QueueUpstreamResult, 1)
	doneCh := make(chan struct{})
	sendCalled := make(chan struct{}, 1)

	c, mt := newStreamTestClient(t)
	mt.OnQueueUpstream(func(_ context.Context) (*transport.QueueUpstreamHandle, error) {
		return &transport.QueueUpstreamHandle{
			Results: resultCh,
			Done:    doneCh,
			SendFn: func(requestID string, msgs []*transport.QueueMessageItem) error {
				sendCalled <- struct{}{}
				resultCh <- &transport.QueueUpstreamResult{
					RefRequestID: requestID,
					Results: []*transport.QueueSendResultItem{
						{MessageID: "msg-1", SentAt: 100},
					},
				}
				return nil
			},
		}, nil
	})

	handle, err := c.QueueUpstream(context.Background())
	require.NoError(t, err)
	require.NotNil(t, handle)

	msgs := []*QueueMessage{
		NewQueueMessage().SetChannel("q-ch").SetBody([]byte("payload")),
	}
	err = handle.Send("req-1", msgs)
	require.NoError(t, err)

	select {
	case <-sendCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for send")
	}

	select {
	case r := <-handle.Results:
		require.NotNil(t, r)
		assert.Equal(t, "req-1", r.RefRequestID)
		require.Len(t, r.Results, 1)
		assert.Equal(t, "msg-1", r.Results[0].MessageID)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for result")
	}

	close(resultCh)
}

func TestClient_QueueUpstream_ClosedClient(t *testing.T) {
	c, _ := newStreamTestClient(t)
	_ = c.Close()

	handle, err := c.QueueUpstream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_QueueUpstream_TransportError(t *testing.T) {
	c, mt := newStreamTestClient(t)
	mt.OnQueueUpstream(func(_ context.Context) (*transport.QueueUpstreamHandle, error) {
		return nil, errors.New("upstream unavailable")
	})

	handle, err := c.QueueUpstream(context.Background())
	require.Error(t, err)
	assert.Nil(t, handle)
	assert.Contains(t, err.Error(), "upstream unavailable")
}

// ---------------------------------------------------------------------------
// NewQueueDownstreamReceiver
// ---------------------------------------------------------------------------

func TestClient_NewQueueDownstreamReceiver_ClosedClient(t *testing.T) {
	c, _ := newStreamTestClient(t)
	_ = c.Close()

	receiver, err := c.NewQueueDownstreamReceiver(context.Background())
	require.Error(t, err)
	assert.Nil(t, receiver)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_NewQueueDownstreamReceiver_TransportError(t *testing.T) {
	c, mt := newStreamTestClient(t)
	mt.OnQueueDownstream(func(_ context.Context) (pb.Kubemq_QueuesDownstreamClient, error) {
		return nil, errors.New("downstream unavailable")
	})

	receiver, err := c.NewQueueDownstreamReceiver(context.Background())
	require.Error(t, err)
	assert.Nil(t, receiver)
	assert.Contains(t, err.Error(), "downstream unavailable")
}

// ---------------------------------------------------------------------------
// PollQueue (via receiver)
// ---------------------------------------------------------------------------

func TestClient_PollQueue_ClosedClient(t *testing.T) {
	c, _ := newStreamTestClient(t)
	_ = c.Close()

	resp, err := c.PollQueue(context.Background(), &PollRequest{
		Channel:            "poll-ch",
		MaxItems:           10,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.ErrorIs(t, err, ErrClientClosed)
}

func TestClient_PollQueue_InvalidChannel(t *testing.T) {
	c, _ := newStreamTestClient(t)

	resp, err := c.PollQueue(context.Background(), &PollRequest{
		Channel:            "invalid channel!@#",
		MaxItems:           10,
		WaitTimeoutSeconds: 1,
	})
	require.Error(t, err)
	assert.Nil(t, resp)
	var kErr *KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestClient_PollQueue_Success(t *testing.T) {
	c, mt := newStreamTestClient(t)

	// Use a channel to safely pass the request ID from Send to Recv.
	reqIDCh := make(chan string, 1)
	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()

	mt.OnQueueDownstream(func(_ context.Context) (pb.Kubemq_QueuesDownstreamClient, error) {
		return &fakeDownstreamClient{
			ctx: streamCtx,
			sendFn: func(req *pb.QueuesDownstreamRequest) error {
				select {
				case reqIDCh <- req.RequestID:
				default:
				}
				return nil
			},
			recvFn: func() (*pb.QueuesDownstreamResponse, error) {
				// Wait for the request ID from Send or context cancellation.
				select {
				case reqID := <-reqIDCh:
					return &pb.QueuesDownstreamResponse{
						TransactionId:   "tx-poll",
						RefRequestId:    reqID,
						RequestTypeData: pb.QueuesDownstreamRequestType_Get,
						Messages: []*pb.QueueMessage{
							{MessageID: "pm-1", Channel: "poll-ch", Body: []byte("poll-data")},
						},
					}, nil
				case <-streamCtx.Done():
					return nil, streamCtx.Err()
				}
			},
		}, nil
	})

	resp, err := c.PollQueue(context.Background(), &PollRequest{
		Channel:            "poll-ch",
		MaxItems:           10,
		WaitTimeoutSeconds: 5,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "pm-1", resp.Messages[0].Message.ID)
}

// fakeDownstreamClient implements pb.Kubemq_QueuesDownstreamClient for testing.
type fakeDownstreamClient struct {
	grpc.ClientStream
	ctx    context.Context
	sendFn func(req *pb.QueuesDownstreamRequest) error
	recvFn func() (*pb.QueuesDownstreamResponse, error)
}

func (f *fakeDownstreamClient) Send(req *pb.QueuesDownstreamRequest) error {
	if f.sendFn != nil {
		return f.sendFn(req)
	}
	return nil
}

func (f *fakeDownstreamClient) Recv() (*pb.QueuesDownstreamResponse, error) {
	if f.recvFn != nil {
		return f.recvFn()
	}
	return nil, errors.New("not implemented")
}

func (f *fakeDownstreamClient) CloseSend() error { return nil }

func (f *fakeDownstreamClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *fakeDownstreamClient) Trailer() metadata.MD {
	return nil
}

func (f *fakeDownstreamClient) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeDownstreamClient) SendMsg(_ interface{}) error {
	return nil
}

func (f *fakeDownstreamClient) RecvMsg(_ interface{}) error {
	return nil
}
