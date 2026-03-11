package kubemq

import "context"

// Subscription represents an active subscription that can be cancelled.
// It is safe for concurrent use — Cancel() can be called from any goroutine.
//
// Canonical definition per TYPE-REGISTRY.md (type #23).
type Subscription struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{}
}

// newSubscription creates a Subscription backed by the given cancel function
// and done channel. The done channel must be closed when the subscription
// goroutine terminates.
func newSubscription(id string, cancel context.CancelFunc, done chan struct{}) *Subscription {
	return &Subscription{
		id:     id,
		cancel: cancel,
		done:   done,
	}
}

// Unsubscribe cancels the subscription and blocks until the subscription
// goroutine has fully terminated. This is a convenience wrapper combining
// Cancel() + <-Done(). Safe to call from any goroutine; calling it multiple
// times is harmless (subsequent calls return immediately after the first
// completes).
func (s *Subscription) Unsubscribe() {
	s.Cancel()
	<-s.Done()
}

// Cancel cancels the subscription. Safe to call multiple times.
// After cancellation, no more callbacks will be dispatched.
func (s *Subscription) Cancel() {
	if s.cancel != nil {
		s.cancel()
	}
}

// IsDone returns true if the subscription has terminated
// (either by cancellation, error, or client close).
func (s *Subscription) IsDone() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// Done returns a channel that is closed when the subscription terminates.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

// ID returns the subscription identifier.
func (s *Subscription) ID() string {
	return s.id
}
