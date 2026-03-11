// Package middleware provides protocol-layer interceptors for the KubeMQ
// transport. It sits between the public Client and the internal Transport,
// handling cross-cutting concerns like retry, authentication, observability,
// and error mapping.
//
// This package is internal — external users cannot import it.
package middleware

// Chain composes multiple interceptors into a single middleware chain.
// Interceptors are applied in order: the first interceptor added is the
// outermost (called first, returns last).
type Chain struct {
	interceptors []Interceptor
}

// Interceptor is a named middleware component.
type Interceptor interface {
	Name() string
}

// NewChain creates a new middleware chain with the given interceptors.
// Nil interceptors are silently ignored.
func NewChain(interceptors ...Interceptor) *Chain {
	filtered := make([]Interceptor, 0, len(interceptors))
	for _, i := range interceptors {
		if i != nil {
			filtered = append(filtered, i)
		}
	}
	return &Chain{interceptors: filtered}
}

// Len returns the number of interceptors in the chain.
func (c *Chain) Len() int {
	return len(c.interceptors)
}
