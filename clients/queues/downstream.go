package queues

import (
	"context"
	"fmt"
	pb "github.com/kubemq-io/protobuf/go"
	"io"
	"sync"
	"time"
)

type downstream struct {
	sync.Mutex
	downstreamCtx       context.Context
	downstreamCancel    context.CancelFunc
	pendingTransactions map[string]*responseHandler
	activeTransactions  map[string]*responseHandler
	requestCh           chan *pb.QueuesDownstreamRequest
	responseCh          chan *pb.QueuesDownstreamResponse
	errCh               chan error
	doneCh              chan bool
	client              pb.KubemqClient
	isClosed            bool
}

func newDownstream(ctx context.Context, client pb.KubemqClient) *downstream {

	u := &downstream{
		Mutex:               sync.Mutex{},
		activeTransactions:  map[string]*responseHandler{},
		pendingTransactions: map[string]*responseHandler{},
		errCh:               make(chan error, 10),
		requestCh:           make(chan *pb.QueuesDownstreamRequest, 10),
		responseCh:          make(chan *pb.QueuesDownstreamResponse, 10),
		doneCh:              make(chan bool, 1),
		client:              client,
	}
	u.downstreamCtx, u.downstreamCancel = context.WithCancel(ctx)
	go u.run()
	return u
}

func (d *downstream) close() {
	d.setIsClose(true)
	d.downstreamCancel()
}
func (d *downstream) setIsClose(value bool) {
	d.Lock()
	defer d.Unlock()
	d.isClosed = value
}
func (d *downstream) getIsClose() bool {
	d.Lock()
	defer d.Unlock()
	return d.isClosed
}
func (d *downstream) createPendingTransaction(request *pb.QueuesDownstreamRequest) *responseHandler {
	d.Lock()
	defer d.Unlock()
	handler := newResponseHandler().
		setRequestId(request.RequestID).
		setRequestChanel(request.Channel).
		setRequestClientId(request.ClientID).
		setRequestCh(d.requestCh)
	d.pendingTransactions[request.RequestID] = handler
	return handler
}
func (d *downstream) movePendingToActiveTransaction(requestId, transactionId string) (*responseHandler, bool) {
	d.Lock()
	defer d.Unlock()
	handler, ok := d.pendingTransactions[requestId]
	if ok {
		handler.transactionId = transactionId
		d.activeTransactions[transactionId] = handler
		delete(d.pendingTransactions, requestId)
		return handler, true
	} else {
		return nil, false
	}
}
func (d *downstream) deletePendingTransaction(requestId string) {
	d.Lock()
	defer d.Unlock()
	delete(d.pendingTransactions, requestId)
	//log.Println("pending transaction deleted", requestId)
}

func (d *downstream) getActiveTransaction(id string) (*responseHandler, bool) {
	d.Lock()
	defer d.Unlock()
	handler, ok := d.activeTransactions[id]
	return handler, ok
}

func (d *downstream) deleteActiveTransaction(id string) {
	d.Lock()
	defer d.Unlock()
	delete(d.activeTransactions, id)
	//log.Println("active transaction deleted", id)
}
func (d *downstream) connectStream(ctx context.Context) {
	defer func() {
		d.doneCh <- true
	}()
	stream, err := d.client.QueuesDownstream(ctx)
	if err != nil {
		d.errCh <- err
		return
	}
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				d.errCh <- err
				return
			}
			select {
			case d.responseCh <- res:
			case <-stream.Context().Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case req := <-d.requestCh:
			err := stream.Send(req)
			if err != nil {
				if err == io.EOF {
					return
				}
				d.errCh <- err
				return
			}
		case <-stream.Context().Done():
			return
		case <-ctx.Done():
			return
		}
	}

}
func (d *downstream) clearPendingTransactions(err error) {
	d.Lock()
	d.Unlock()
	for id, handler := range d.pendingTransactions {
		handler.sendError(err)
		handler.sendComplete()
		delete(d.pendingTransactions, id)
	}
}
func (d *downstream) clearActiveTransactions(err error) {
	d.Lock()
	d.Unlock()
	for id, handler := range d.activeTransactions {
		handler.sendError(err)
		handler.sendComplete()
		delete(d.activeTransactions, id)

	}
}
func (d *downstream) run() {
	for {
		if !d.getIsClose() {
			go d.connectStream(d.downstreamCtx)
		} else {
			return
		}
		for {
			select {
			case resp := <-d.responseCh:
				if resp.RequestTypeData == pb.QueuesDownstreamRequestType_Get {
					handler, ok := d.movePendingToActiveTransaction(resp.RefRequestId, resp.TransactionId)
					if ok {
						handler.responseCh <- resp
					}
				} else {
					handler, ok := d.getActiveTransaction(resp.TransactionId)
					if ok {
						if resp.TransactionComplete {
							handler.sendComplete()
							d.deleteActiveTransaction(resp.TransactionId)
							continue
						}
						if resp.IsError {
							handler.sendError(fmt.Errorf(resp.Error))
							continue
						}
					}
				}
			case err := <-d.errCh:
				d.clearPendingTransactions(err)
				d.clearActiveTransactions(err)
			case <-d.doneCh:
				goto reconnect
			case <-d.downstreamCtx.Done():
				d.clearPendingTransactions(d.downstreamCtx.Err())
				d.clearActiveTransactions(d.downstreamCtx.Err())
				return
			}
		}
	reconnect:
		time.Sleep(time.Second)
	}
}

func (d *downstream) poll(ctx context.Context, request *PollRequest, clientId string) (*PollResponse, error) {
	pbReq, err := request.validateAndComplete(clientId)
	if err != nil {
		return nil, err
	}
	respHandler := d.createPendingTransaction(pbReq).
		setOnErrorFunc(request.OnErrorFunc).
		setOnCompleteFunc(request.OnComplete)
	//log.Println("response handler created")
	d.requestCh <- pbReq
	//log.Println("request sent")
	select {
	case resp := <-respHandler.responseCh:
		//log.Println("first response accepted")
		if resp.IsError {
			//log.Println("first response error", resp.Error)
			return nil, fmt.Errorf(resp.Error)
		}
		pollResponse := newPollResponse(resp.Messages, respHandler)
		if len(pollResponse.Messages) > 0 && !pbReq.AutoAck {
			//log.Println("first response, we have messages and we are in manual ack")
			respHandler.start(ctx)
		} else {
			//log.Println("no messages or in autoack")
			respHandler.sendComplete()
			d.deleteActiveTransaction(resp.TransactionId)
		}
		return pollResponse, nil
	case <-d.downstreamCtx.Done():
		respHandler.sendError(d.downstreamCtx.Err())
		respHandler.sendComplete()
		d.deletePendingTransaction(pbReq.RequestID)
		return nil, d.downstreamCtx.Err()
	case <-ctx.Done():
		respHandler.sendError(ctx.Err())
		respHandler.sendComplete()
		d.deletePendingTransaction(pbReq.RequestID)
		return nil, ctx.Err()
	}
}
