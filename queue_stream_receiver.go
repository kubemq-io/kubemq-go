package kubemq

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kubemq-io/kubemq-go/v2/internal/transport"
	pb "github.com/kubemq-io/kubemq-go/v2/pb"
	"github.com/kubemq-io/kubemq-go/v2/pkg/uuid"
)

// QueueDownstreamReceiver manages a persistent queue downstream stream with
// automatic reconnection. It exposes a Poll() method that returns a PollResponse
// containing the received messages.
//
// Create one using Client.NewQueueDownstreamReceiver(). Call Poll() in a loop
// to continuously receive messages. Call Close() when done.
type QueueDownstreamReceiver struct {
	transport transport.Transport
	clientID  string
	logger    Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu           sync.Mutex
	stream       pb.Kubemq_QueuesDownstreamClient
	sendDone     <-chan error
	streamCancel context.CancelFunc // per-stream context cancel — kills send goroutine on reconnect

	sendCh     chan *pb.QueuesDownstreamRequest  // capacity 512
	responseCh chan *pb.QueuesDownstreamResponse // capacity 1, Get responses
	errCh      chan error                        // capacity 8, non-blocking sends only

	stopCh   chan struct{}
	stopOnce sync.Once
	closed   atomic.Bool

	pollMu      sync.Mutex
	reconnectCh chan struct{} // closed on disconnect to unblock pending Poll, recreated after reconnect

	txnMu    sync.Mutex
	openTxns map[string]struct{}

	openStream func() (pb.Kubemq_QueuesDownstreamClient, error)
}

// startSendLoop reads from sendCh and writes to the gRPC stream.
// Returns a done channel that is closed when the send goroutine exits.
func (r *QueueDownstreamReceiver) startSendLoop(streamCtx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() {
		defer close(done)
		for {
			select {
			case <-r.stopCh:
				r.drainSendCh()
				return
			case <-streamCtx.Done():
				// Per-stream cancel — recv goroutine is reconnecting.
				// Exit immediately so recv goroutine can proceed.
				return
			case <-r.ctx.Done():
				return
			case pbReq := <-r.sendCh:
				r.mu.Lock()
				s := r.stream
				r.mu.Unlock()
				if err := s.Send(pbReq); err != nil {
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		}
	}()
	return done
}

// drainSendCh sends all pending messages from sendCh, stopping on first error.
func (r *QueueDownstreamReceiver) drainSendCh() {
	for {
		select {
		case pbReq := <-r.sendCh:
			r.mu.Lock()
			s := r.stream
			r.mu.Unlock()
			if err := s.Send(pbReq); err != nil {
				return
			}
		default:
			return
		}
	}
}

// recvLoop is the response reader goroutine. It reads from the gRPC stream,
// routes Get responses to responseCh, errors to errCh, and handles reconnection.
func (r *QueueDownstreamReceiver) recvLoop() {
	defer r.cancel()
	defer close(r.errCh)
	defer close(r.responseCh)

	streamCtx, streamCancel := context.WithCancel(r.ctx)
	sendDone := r.startSendLoop(streamCtx)
	r.mu.Lock()
	r.sendDone = sendDone
	r.streamCancel = streamCancel
	r.mu.Unlock()

	for {
		select {
		case <-sendDone:
		default:
		}

		resp, recvErr := r.stream.Recv()
		if recvErr != nil {
			select {
			case <-r.ctx.Done():
				return
			default:
			}

			if !r.transport.IsConnectionError(recvErr) {
				select {
				case r.errCh <- recvErr:
				default:
					r.logger.Warn("error channel full, discarding", "error", recvErr)
				}
				return
			}

			// --- Reconnection path ---
			r.logger.Info("queue downstream stream lost, waiting for reconnection")
			select {
			case r.errCh <- fmt.Errorf("kubemq: queue downstream stream reconnecting: %w", recvErr):
			default:
			}

			// 1. Signal pending Poll() to fail immediately
			close(r.reconnectCh)

			// 2. Kill send goroutine via per-stream context (not receiver context)
			streamCancel()
			<-sendDone // now guaranteed to complete — streamCtx is cancelled

			// 3. Discard all stale items from sendCh.
			//    Everything in the buffer is from the dead stream:
			//    - Settlements: transactions already NACKed by server on disconnect.
			//    - Gets: would create phantom transactions on the new stream.
			//    This is safe because Poll() already returned via reconnectCh
			//    and settlement callers already received nil (fire-and-forget).
			discarded := 0
			for {
				select {
				case <-r.sendCh:
					discarded++
				default:
					goto drained
				}
			}
		drained:
			if discarded > 0 {
				r.logger.Info("discarded stale requests after reconnect", "count", discarded)
			}

			// 4. Wait for transport to reconnect
			waitCh := r.transport.WaitReconnect()
			select {
			case <-r.ctx.Done():
				return
			case <-waitCh:
			}

			// 5. Re-open stream
			r.logger.Info("reconnection detected, re-opening queue downstream stream")
			newStream, openErr := r.openStream()
			if openErr != nil {
				r.logger.Error("queue downstream stream re-open failed", "error", openErr)
				select {
				case r.errCh <- fmt.Errorf("queue downstream recovery failed: %w", openErr):
				default:
				}
				return
			}

			// 6. Swap stream, restart send goroutine, recreate reconnectCh
			streamCtx, streamCancel = context.WithCancel(r.ctx)
			r.mu.Lock()
			r.stream = newStream
			r.mu.Unlock()

			sendDone = r.startSendLoop(streamCtx)

			r.mu.Lock()
			r.sendDone = sendDone
			r.streamCancel = streamCancel
			r.reconnectCh = make(chan struct{})
			r.mu.Unlock()

			continue
		}

		// --- Response routing (all errCh sends non-blocking) ---

		if pb.QueuesDownstreamRequestType(resp.RequestTypeData) == pb.QueuesDownstreamRequestType_CloseByServer {
			select {
			case r.errCh <- fmt.Errorf("kubemq: server closed downstream stream"):
			default:
			}
			return
		}

		if resp.IsError {
			select {
			case r.errCh <- fmt.Errorf("kubemq: queue downstream error: %s", resp.Error):
			default:
				r.logger.Warn("error channel full, discarding", "error", resp.Error)
			}
			if pb.QueuesDownstreamRequestType(resp.RequestTypeData) == pb.QueuesDownstreamRequestType_Get {
				select {
				case r.responseCh <- resp:
				case <-r.ctx.Done():
				}
			}
			continue
		}

		if pb.QueuesDownstreamRequestType(resp.RequestTypeData) == pb.QueuesDownstreamRequestType_Get {
			select {
			case r.responseCh <- resp:
			case <-r.ctx.Done():
			}
			continue
		}

		r.logger.Warn("unexpected downstream response type", "type", resp.RequestTypeData)
	}
}

// Poll sends a poll request and waits for the server response, returning
// a PollResponse containing the received messages. The returned PollResponse
// provides settlement methods (AckAll, NackAll, ReQueueAll), and each message
// provides individual settlement methods (Ack, Nack, ReQueue).
//
// Poll blocks until a response is received, the context is cancelled, or the
// receiver is closed.
func (r *QueueDownstreamReceiver) Poll(ctx context.Context, req *PollRequest) (*PollResponse, error) {
	if r.closed.Load() {
		return nil, fmt.Errorf("kubemq: receiver closed")
	}

	r.pollMu.Lock()
	defer r.pollMu.Unlock()

	if req.Channel == "" {
		return nil, fmt.Errorf("kubemq: channel cannot be empty")
	}
	if req.MaxItems <= 0 {
		req.MaxItems = 1
	}
	waitTimeoutMs := req.WaitTimeoutSeconds * 1000
	if waitTimeoutMs < 1000 {
		waitTimeoutMs = 1000
	}

	requestID := uuid.New()
	pbReq := &pb.QueuesDownstreamRequest{
		RequestID:       requestID,
		ClientID:        r.clientID,
		RequestTypeData: pb.QueuesDownstreamRequestType_Get,
		Channel:         req.Channel,
		MaxItems:        req.MaxItems,
		WaitTimeout:     waitTimeoutMs,
		AutoAck:         req.AutoAck,
	}

	// Snapshot reconnectCh under lock for this poll cycle
	r.mu.Lock()
	reconnectCh := r.reconnectCh
	r.mu.Unlock()

	select {
	case r.sendCh <- pbReq:
	case <-reconnectCh:
		// Built-in delay to prevent callers without backoff from spinning at CPU speed
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
		return nil, fmt.Errorf("kubemq: stream disconnected, retry poll")
	case <-r.stopCh:
		return nil, fmt.Errorf("kubemq: receiver closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	for {
		select {
		case resp, ok := <-r.responseCh:
			if !ok {
				return nil, fmt.Errorf("kubemq: receiver closed")
			}
			if resp.RefRequestId != requestID {
				continue // stale response — discard
			}
			return r.buildPollResponse(resp, req.AutoAck), nil

		case <-reconnectCh:
			// Built-in delay to prevent callers without backoff from spinning at CPU speed
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
			return nil, fmt.Errorf("kubemq: stream disconnected, retry poll")

		case <-r.stopCh:
			return nil, fmt.Errorf("kubemq: receiver closed")

		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// buildPollResponse converts a proto response into a PollResponse.
func (r *QueueDownstreamReceiver) buildPollResponse(
	resp *pb.QueuesDownstreamResponse,
	autoAck bool,
) *PollResponse {
	result := &PollResponse{
		TransactionID: resp.TransactionId,
		IsError:       resp.IsError,
		Error:         resp.Error,
		autoAck:       autoAck,
	}

	if resp.IsError || len(resp.Messages) == 0 {
		return result
	}

	var sendFn func(req settleRequest) error
	if !autoAck {
		r.txnMu.Lock()
		r.openTxns[resp.TransactionId] = struct{}{}
		r.txnMu.Unlock()

		sendFn = r.settle
		txnID := resp.TransactionId
		result.cleanupTxn = func() {
			r.txnMu.Lock()
			delete(r.openTxns, txnID)
			r.txnMu.Unlock()
		}
	}
	result.sendFn = sendFn

	result.Messages = make([]*QueueDownstreamMessage, 0, len(resp.Messages))
	for _, pbMsg := range resp.Messages {
		qm := &QueueMessage{
			ID:       pbMsg.MessageID,
			ClientID: pbMsg.ClientID,
			Channel:  pbMsg.Channel,
			Metadata: pbMsg.Metadata,
			Body:     pbMsg.Body,
			Tags:     pbMsg.Tags,
		}
		if pbMsg.Policy != nil {
			qm.Policy = &QueuePolicy{
				DelaySeconds:      int(pbMsg.Policy.DelaySeconds),
				ExpirationSeconds: int(pbMsg.Policy.ExpirationSeconds),
				MaxReceiveCount:   int(pbMsg.Policy.MaxReceiveCount),
				MaxReceiveQueue:   pbMsg.Policy.MaxReceiveQueue,
			}
		}
		var seq uint64
		if pbMsg.Attributes != nil {
			seq = pbMsg.Attributes.Sequence
			qm.Attributes = &QueueMessageAttributes{
				Timestamp:         pbMsg.Attributes.Timestamp,
				Sequence:          pbMsg.Attributes.Sequence,
				MD5OfBody:         pbMsg.Attributes.MD5OfBody,
				ReceiveCount:      int(pbMsg.Attributes.ReceiveCount),
				ReRouted:          pbMsg.Attributes.ReRouted,
				ReRoutedFromQueue: pbMsg.Attributes.ReRoutedFromQueue,
				ExpirationAt:      pbMsg.Attributes.ExpirationAt,
				DelayedTo:         pbMsg.Attributes.DelayedTo,
			}
		}
		result.Messages = append(result.Messages, &QueueDownstreamMessage{
			Message:       qm,
			TransactionID: resp.TransactionId,
			Sequence:      seq,
			autoAck:       autoAck,
			sendFn:        sendFn,
		})
	}

	return result
}

// settle enqueues a settlement request on the send channel.
func (r *QueueDownstreamReceiver) settle(req settleRequest) error {
	pbReq := &pb.QueuesDownstreamRequest{
		RequestID:        uuid.New(),
		ClientID:         r.clientID,
		RequestTypeData:  pb.QueuesDownstreamRequestType(req.requestType),
		RefTransactionId: req.transactionID,
		SequenceRange:    req.sequences,
		ReQueueChannel:   req.reQueueChannel,
	}
	return r.enqueue(pbReq)
}

// enqueue sends a proto request through sendCh with closed-state and context guards.
func (r *QueueDownstreamReceiver) enqueue(req *pb.QueuesDownstreamRequest) error {
	if r.closed.Load() {
		return fmt.Errorf("kubemq: receiver closed")
	}
	select {
	case r.sendCh <- req:
		return nil
	case <-r.stopCh:
		return fmt.Errorf("kubemq: receiver closed")
	case <-r.ctx.Done():
		return fmt.Errorf("kubemq: receiver closed")
	}
}

// Errors returns a channel that receives non-fatal errors (e.g., reconnection notices).
func (r *QueueDownstreamReceiver) Errors() <-chan error {
	return r.errCh
}

// Close terminates the downstream receiver and releases all resources.
func (r *QueueDownstreamReceiver) Close() error {
	r.stopOnce.Do(func() {
		r.closed.Store(true)

		// 1. Enqueue CloseByClient for each open transaction (best-effort).
		//    Server Dispose() handles cleanup on stream close for any we miss.
		r.txnMu.Lock()
		txns := make([]string, 0, len(r.openTxns))
		for txnID := range r.openTxns {
			txns = append(txns, txnID)
		}
		r.openTxns = make(map[string]struct{}) // empty map, not nil
		r.txnMu.Unlock()

		for _, txnID := range txns {
			select {
			case r.sendCh <- &pb.QueuesDownstreamRequest{
				RequestID:        uuid.New(),
				ClientID:         r.clientID,
				RequestTypeData:  pb.QueuesDownstreamRequestType_CloseByClient,
				RefTransactionId: txnID,
			}:
			default:
				// sendCh full — server handles cleanup on stream close
			}
		}

		// 2. Signal send goroutine to drain and exit
		close(r.stopCh)

		// 3. Wait for drain (bounded)
		r.mu.Lock()
		sd := r.sendDone
		r.mu.Unlock()
		if sd != nil {
			select {
			case <-sd:
			case <-time.After(5 * time.Second):
			}
		}

		// 4. Graceful half-close (before context cancel)
		r.mu.Lock()
		s := r.stream
		r.mu.Unlock()
		if s != nil {
			_ = s.CloseSend()
		}

		// 5. Cancel context — tears down recv goroutine
		r.cancel()
	})
	return nil
}
