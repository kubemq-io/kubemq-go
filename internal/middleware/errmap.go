package middleware

import (
	"context"
	"errors"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type errmapInterceptor struct{}

// NewErrmapInterceptor creates an interceptor that maps gRPC status errors
// to *types.KubeMQError for all unary and streaming calls.
func NewErrmapInterceptor() *errmapInterceptor {
	return &errmapInterceptor{}
}

func (e *errmapInterceptor) Name() string { return "errmap" }

func (e *errmapInterceptor) UnaryInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err == nil {
			return nil
		}
		return e.mapError(ctx, method, err)
	}
}

func (e *errmapInterceptor) StreamInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			return nil, e.mapError(ctx, method, err)
		}
		return &mappedStream{ClientStream: cs, ctx: ctx, method: method, mapper: e}, nil
	}
}

func (e *errmapInterceptor) mapError(ctx context.Context, method string, err error) error {
	var kErr *types.KubeMQError
	if errors.As(err, &kErr) {
		return err
	}

	st, ok := status.FromError(err)
	if !ok {
		return &types.KubeMQError{
			Code:        types.ErrCodeFatal,
			Message:     err.Error(),
			Operation:   method,
			IsRetryable: false,
			Cause:       err,
		}
	}

	cat, code := types.ClassifyGRPCCode(st.Code())

	if st.Code() == codes.Canceled {
		if ctx.Err() != nil {
			cat = types.CategoryCancellation
			code = types.ErrCodeCancellation
		} else {
			cat = types.CategoryTransient
			code = types.ErrCodeTransient
		}
	}

	return &types.KubeMQError{
		Code:        code,
		Message:     st.Message(),
		Operation:   method,
		IsRetryable: types.IsRetryableCategory(cat),
		Cause:       err,
	}
}

type mappedStream struct {
	grpc.ClientStream
	ctx    context.Context
	method string
	mapper *errmapInterceptor
}

func (s *mappedStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		return nil
	}
	return s.mapper.mapError(s.ctx, s.method, err)
}
