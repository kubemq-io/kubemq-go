package middleware

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testInterceptor struct {
	name string
}

func (t *testInterceptor) Name() string { return t.name }

func TestNewChain(t *testing.T) {
	t.Run("creates chain with interceptors", func(t *testing.T) {
		a := &testInterceptor{name: "a"}
		b := &testInterceptor{name: "b"}
		chain := NewChain(a, b)
		assert.Equal(t, 2, chain.Len())
	})

	t.Run("filters nil interceptors", func(t *testing.T) {
		a := &testInterceptor{name: "a"}
		chain := NewChain(a, nil, nil)
		assert.Equal(t, 1, chain.Len())
	})

	t.Run("empty chain", func(t *testing.T) {
		chain := NewChain()
		assert.Equal(t, 0, chain.Len())
	})
}
