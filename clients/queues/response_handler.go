package queues

import (
	"context"
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	pb "github.com/kubemq-io/protobuf/go"
	"go.uber.org/atomic"
	"sync"
	"time"
)

const requestTimout = 60 * time.Second

type responseHandler struct {
	sync.Mutex
	isActive *atomic.Bool

	handlerCtx      context.Context
	handlerCancel   context.CancelFunc
	activeOffsets   map[int64]struct{}
	responseCh      chan *pb.QueuesDownstreamResponse
	requestCh       chan *pb.QueuesDownstreamRequest
	requestClientId string
	requestChannel  string
	transactionId   string
	requestId       string
	onDoneFunc      func(transactionId string)
}

func (r *responseHandler) setOnDoneFunc(onDoneFunc func(transactionId string)) *responseHandler {
	r.onDoneFunc = onDoneFunc
	return r
}

func newResponseHandler() *responseHandler {
	return &responseHandler{
		isActive:      atomic.NewBool(false),
		activeOffsets: map[int64]struct{}{},
		responseCh:    make(chan *pb.QueuesDownstreamResponse, 10),
	}
}
func (r *responseHandler) activeMessagesCount() int {
	r.Lock()
	r.Unlock()
	return len(r.activeOffsets)
}
func (r *responseHandler) deleteOffsets(offsets ...int64) {
	r.Lock()
	r.Unlock()
	for i := 0; i < len(offsets); i++ {
		delete(r.activeOffsets, offsets[i])
	}
}
func (r *responseHandler) deleteAllOffsets() {
	r.Lock()
	r.Unlock()
	r.activeOffsets = map[int64]struct{}{}
}
func (r *responseHandler) start(ctx context.Context, offsets []int64) {
	for _, offset := range offsets {
		r.activeOffsets[offset] = struct{}{}
	}
	r.handlerCtx, r.handlerCancel = context.WithCancel(ctx)
	r.isActive.Store(true)
	//log.Println("response handler started", r.transactionId)
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
	r.deleteAllOffsets()
	r.checkAndCompleteTransaction()
	return nil
}
func (r *responseHandler) closeFromServer() {
	r.deleteAllOffsets()
	r.checkAndCompleteTransaction()
}
func (r *responseHandler) sendRequest(request *pb.QueuesDownstreamRequest) error {
	if !r.isActive.Load() {
		return fmt.Errorf("current transaction is not active")
	}
	r.Lock()
	defer r.Unlock()
	select {
	case r.requestCh <- request:
		//log.Println("sending request", request.RequestTypeData, request.RequestID)
	case <-time.After(requestTimout):
		return fmt.Errorf("request send timeout")
	case <-r.handlerCtx.Done():
		r.isActive.Store(false)
		//log.Println("response ctx handler canceled", r.transactionId)
		return r.handlerCtx.Err()
	}
	select {
	case resp := <-r.responseCh:
		if resp.IsError {
			//log.Println("get response error", request.RequestTypeData, resp.RefRequestId, resp.Error)
			return fmt.Errorf(resp.Error)
		}
		//log.Println("get response", request.RequestTypeData, resp.RefRequestId)
		return nil
	case <-time.After(requestTimout):
		return fmt.Errorf("response timeout")
	case <-r.handlerCtx.Done():
		r.isActive.Store(false)
		//log.Println("response ctx handlers canceled", r.transactionId)
		return r.handlerCtx.Err()
	}

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

func (r *responseHandler) checkAndCompleteTransaction() {
	if r.activeMessagesCount() == 0 {
		r.isActive.Store(false)
		r.handlerCancel()
		r.onDoneFunc(r.transactionId)
		//log.Println("check complete found no active messages, closed the handler")
	} else {
		//log.Println("check complete found active messages", r.activeMessagesCount())
	}
}
func (r *responseHandler) AckAll() error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to ack")
	}
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
	r.deleteAllOffsets()
	r.checkAndCompleteTransaction()
	return nil
}
func (r *responseHandler) AckOffsets(offsets ...int64) error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to ack")
	}
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
	r.deleteOffsets(offsets...)
	r.checkAndCompleteTransaction()
	return nil
}

func (r *responseHandler) NAckAll() error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to nack")
	}
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
	r.deleteAllOffsets()
	r.checkAndCompleteTransaction()
	return nil
}
func (r *responseHandler) NAckOffsets(offsets ...int64) error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to nack")
	}
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
	r.deleteOffsets(offsets...)
	r.checkAndCompleteTransaction()
	return nil
}
func (r *responseHandler) ReQueueAll(channel string) error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to requeue")
	}
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
	r.deleteAllOffsets()
	r.checkAndCompleteTransaction()
	return nil
}
func (r *responseHandler) ReQueueOffsets(channel string, offsets ...int64) error {
	if r.activeMessagesCount() == 0 {
		return fmt.Errorf("no active messages found to requeue")
	}
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
	r.deleteOffsets(offsets...)
	r.checkAndCompleteTransaction()
	return nil
}
