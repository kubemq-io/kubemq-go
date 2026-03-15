package middleware

import (
	"context"
	"testing"

	"github.com/kubemq-io/kubemq-go/v2/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestAuthInterceptor_Name(t *testing.T) {
	provider := types.NewStaticTokenProvider("token")
	ai := NewAuthInterceptor(context.Background(), provider, &testLogger{})
	defer ai.Close()
	assert.Equal(t, "auth", ai.Name())
}
