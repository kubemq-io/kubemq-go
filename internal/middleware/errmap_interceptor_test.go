package middleware

import (
	"context"
	"errors"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrmapUnaryInterceptor_NilError(t *testing.T) {
	interceptor := NewErrmapInterceptor().UnaryInterceptor()
	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return nil
	}
	err := interceptor(context.Background(), "/test/Method", nil, nil, nil, mockInvoker)
	assert.NoError(t, err)
}

func TestErrmapUnaryInterceptor_GRPCError(t *testing.T) {
	interceptor := NewErrmapInterceptor().UnaryInterceptor()
	grpcErr := status.Error(codes.NotFound, "not found")
	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return grpcErr
	}
	err := interceptor(context.Background(), "/test/Method", nil, nil, nil, mockInvoker)
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeNotFound, kErr.Code)
	assert.Equal(t, "/test/Method", kErr.Operation)
}

func TestErrmapUnaryInterceptor_NonGRPCError(t *testing.T) {
	interceptor := NewErrmapInterceptor().UnaryInterceptor()
	plainErr := errors.New("plain error")
	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return plainErr
	}
	err := interceptor(context.Background(), "/test/Method", nil, nil, nil, mockInvoker)
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeFatal, kErr.Code)
	assert.False(t, kErr.IsRetryable)
}

func TestErrmapUnaryInterceptor_CanceledWithCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	interceptor := NewErrmapInterceptor().UnaryInterceptor()
	grpcErr := status.Error(codes.Canceled, "cancelled")
	mockInvoker := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		return grpcErr
	}
	err := interceptor(ctx, "/test/Method", nil, nil, nil, mockInvoker)
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.Equal(t, types.ErrCodeCancellation, kErr.Code)
	assert.False(t, kErr.IsRetryable)
}

func TestErrmapStreamInterceptor_Error(t *testing.T) {
	streamInterceptor := NewErrmapInterceptor().StreamInterceptor()
	grpcErr := status.Error(codes.Unavailable, "connection refused")
	mockStreamer := func(_ context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, grpcErr
	}
	stream, err := streamInterceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test/Method", mockStreamer)
	assert.Nil(t, stream)
	require.Error(t, err)
	var kErr *types.KubeMQError
	require.True(t, errors.As(err, &kErr))
	assert.True(t, kErr.IsRetryable)
}

func TestErrmapStreamInterceptor_Success(t *testing.T) {
	streamInterceptor := NewErrmapInterceptor().StreamInterceptor()
	mockStream := &mockClientStream{recvErr: nil}
	mockStreamer := func(_ context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
		return mockStream, nil
	}
	stream, err := streamInterceptor(context.Background(), &grpc.StreamDesc{}, nil, "/test/Method", mockStreamer)
	require.NoError(t, err)
	require.NotNil(t, stream)
	ms, ok := stream.(*mappedStream)
	require.True(t, ok)
	assert.Same(t, mockStream, ms.ClientStream)
	assert.Equal(t, "/test/Method", ms.method)
}
