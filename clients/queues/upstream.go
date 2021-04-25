package queues

import (
	"context"
	pb "github.com/kubemq-io/protobuf/go"
	"io"
	"sync"
	"time"
)

type upstream struct {
	sync.Mutex
	upstreamCtx        context.Context
	upstreamCancel     context.CancelFunc
	activeTransactions map[string]chan *pb.QueuesUpstreamResponse
	requestCh          chan *pb.QueuesUpstreamRequest
	responseCh         chan *pb.QueuesUpstreamResponse
	errCh              chan error
	doneCh             chan bool
	client             pb.KubemqClient
	isClosed           bool
}

func newUpstream(ctx context.Context, client pb.KubemqClient) *upstream {

	u := &upstream{
		Mutex:              sync.Mutex{},
		activeTransactions: map[string]chan *pb.QueuesUpstreamResponse{},
		errCh:              make(chan error, 10),
		requestCh:          make(chan *pb.QueuesUpstreamRequest, 10),
		responseCh:         make(chan *pb.QueuesUpstreamResponse, 10),
		doneCh:             make(chan bool, 1),
		client:             client,
	}
	u.upstreamCtx, u.upstreamCancel = context.WithCancel(ctx)
	go u.run()
	return u
}

func (u *upstream) close() {
	u.setIsClose(true)
	u.upstreamCancel()
}
func (u *upstream) setIsClose(value bool) {
	u.Lock()
	defer u.Unlock()
	u.isClosed = value
}
func (u *upstream) getIsClose() bool {
	u.Lock()
	defer u.Unlock()
	return u.isClosed
}

func (u *upstream) setTransaction(id string) chan *pb.QueuesUpstreamResponse {
	u.Lock()
	defer u.Unlock()
	respCh := make(chan *pb.QueuesUpstreamResponse, 1)
	u.activeTransactions[id] = respCh
	return respCh
}
func (u *upstream) getTransaction(id string) (chan *pb.QueuesUpstreamResponse, bool) {
	u.Lock()
	defer u.Unlock()
	respCh, ok := u.activeTransactions[id]
	return respCh, ok

}
func (u *upstream) deleteTransaction(id string) {
	u.Lock()
	defer u.Unlock()
	delete(u.activeTransactions, id)
}
func (u *upstream) connectStream(ctx context.Context) {
	defer func() {
		u.doneCh <- true
	}()
	stream, err := u.client.QueuesUpstream(ctx)

	if err != nil {
		u.errCh <- err
		return
	}
	go func() {
		for {
			res, err := stream.Recv()
			if err != nil {
				if err == io.EOF {
					return
				}
				u.errCh <- err
				return
			}
			select {
			case u.responseCh <- res:
			case <-stream.Context().Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case req := <-u.requestCh:
			err := stream.Send(req)
			if err != nil {
				if err == io.EOF {
					return
				}
				u.errCh <- err
				return
			}
		case <-stream.Context().Done():
			return
		case <-ctx.Done():
			return
		}
	}

}
func (u *upstream) clearPendingTransactions(err error) {
	u.Lock()
	u.Unlock()
	for id, respCh := range u.activeTransactions {
		respCh <- &pb.QueuesUpstreamResponse{
			RefRequestID: id,
			Results:      nil,
			IsError:      true,
			Error:        err.Error(),
		}
		delete(u.activeTransactions, id)
	}
}
func (u *upstream) run() {
	for {
		if !u.getIsClose() {
			go u.connectStream(u.upstreamCtx)
		} else {
			return
		}
		for {
			select {
			case resp := <-u.responseCh:
				respCh, ok := u.getTransaction(resp.RefRequestID)
				if ok {
					respCh <- resp
					u.deleteTransaction(resp.RefRequestID)
				}
			case err := <-u.errCh:
				u.clearPendingTransactions(err)
			case <-u.doneCh:
				goto reconnect
			case <-u.upstreamCtx.Done():
				u.clearPendingTransactions(u.upstreamCtx.Err())
				return
			}
		}
	reconnect:
		time.Sleep(time.Second)
	}
}

func (u *upstream) send(req *pb.QueuesUpstreamRequest) chan *pb.QueuesUpstreamResponse {
	respCh := u.setTransaction(req.RequestID)
	u.requestCh <- req
	return respCh
}
func (u *upstream) cancelTransaction(id string) {
	u.deleteTransaction(id)
}
