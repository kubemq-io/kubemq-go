package queues

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"time"
)

const requestTimout = 60 * time.Second

type responseHandler struct {
	handlerCtx      context.Context
	handlerCancel   context.CancelFunc
	requestCh       chan *pb.QueuesDownstreamRequest
	responseCh      chan *pb.QueuesDownstreamResponse
	isActive        *atomic.Bool
	requestClientId string
	requestChannel  string
	transactionId   string
	requestId       string
	onErrorFunc     func(err error)
	onCompleteFunc  func()
}

func newResponseHandler() *responseHandler {
	return &responseHandler{
		responseCh: make(chan *pb.QueuesDownstreamResponse, 1),
		isActive:   atomic.NewBool(false),
	}
}

func (r *responseHandler) Close() error {
	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_CloseByClient,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   "",
		SequenceRange:    nil,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
func (r *responseHandler) start(ctx context.Context) {
	r.handlerCtx, r.handlerCancel = context.WithCancel(ctx)
	r.isActive.Store(true)
}
func (r *responseHandler) sendRequest(request *pb.QueuesDownstreamRequest) error {
	if !r.isActive.Load() {
		return fmt.Errorf("transaction is not ready to accept requests")
	}

	select {
	case r.requestCh <- request:
		//log.Println("sending request", request.RequestTypeData, request.RequestID)
	case <-time.After(requestTimout):
		return fmt.Errorf("request send timeout")
	case <-r.handlerCtx.Done():
		//log.Println("response ctx handler canceled", r.transactionId)
		return r.handlerCtx.Err()
	}
	return nil
}
func (r *responseHandler) setRequestCh(requestCh chan *pb.QueuesDownstreamRequest) *responseHandler {
	r.requestCh = requestCh
	return r
}

func (r *responseHandler) setRequestClientId(requestClientId string) *responseHandler {
	r.requestClientId = requestClientId
	return r
}
func (r *responseHandler) setRequestChanel(requestChannel string) *responseHandler {
	r.requestChannel = requestChannel
	return r
}
func (r *responseHandler) setTransactionId(transactionId string) *responseHandler {
	r.transactionId = transactionId
	return r
}

func (r *responseHandler) setRequestId(requestId string) *responseHandler {
	r.requestId = requestId
	return r
}
func (r *responseHandler) setOnErrorFunc(onErrorFunc func(err error)) *responseHandler {
	r.onErrorFunc = onErrorFunc
	return r
}

func (r *responseHandler) setOnCompleteFunc(onCloseFunc func()) *responseHandler {

	r.onCompleteFunc = onCloseFunc
	return r
}
func (r *responseHandler) sendError(err error) {
	if r.onErrorFunc != nil {
		r.onErrorFunc(err)
	}
}
func (r *responseHandler) sendComplete() {

	r.isActive.Store(false)
	if r.handlerCancel != nil {
		r.handlerCancel()
	}
	if r.onCompleteFunc != nil {
		r.onCompleteFunc()
	}
}
func (r *responseHandler) AckAll() error {
	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_AckAll,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   "",
		SequenceRange:    nil,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
func (r *responseHandler) AckOffsets(offsets ...int64) error {

	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_AckRange,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   "",
		SequenceRange:    offsets,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}

func (r *responseHandler) NAckAll() error {
	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_NAckAll,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   "",
		SequenceRange:    nil,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
func (r *responseHandler) NAckOffsets(offsets ...int64) error {
	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_NAckRange,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   "",
		SequenceRange:    offsets,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
func (r *responseHandler) ReQueueAll(channel string) error {
	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_ReQueueAll,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   channel,
		SequenceRange:    nil,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
func (r *responseHandler) ReQueueOffsets(channel string, offsets ...int64) error {

	request := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.requestClientId,
		RequestTypeData:  pb.QueuesDownstreamRequestType_ReQueueRange,
		Channel:          r.requestChannel,
		MaxItems:         0,
		WaitTimeout:      0,
		AutoAck:          false,
		ReQueueChannel:   channel,
		SequenceRange:    offsets,
		RefTransactionId: r.transactionId,
	}
	err := r.sendRequest(request)
	if err != nil {
		return err
	}
	return nil
}
