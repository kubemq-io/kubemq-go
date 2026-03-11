package kubemq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStaticTokenProvider(t *testing.T) {
	p := NewStaticTokenProvider("my-secret-token")

	token, expiresAt, err := p.GetToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "my-secret-token", token)
	assert.True(t, expiresAt.IsZero())
}

func TestStaticTokenProvider_ImplementsCredentialProvider(t *testing.T) {
	var _ CredentialProvider = NewStaticTokenProvider("token")
}

func TestWithAuthToken_CreatesStaticProvider(t *testing.T) {
	opts := GetDefaultOptions()
	WithAuthToken("test-token").apply(opts)

	require.NotNil(t, opts.credentialProvider)
	token, _, err := opts.credentialProvider.GetToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "test-token", token)
}

func TestWithCredentialProvider_OverridesAuthToken(t *testing.T) {
	opts := GetDefaultOptions()
	WithAuthToken("static").apply(opts)

	custom := NewStaticTokenProvider("custom")
	WithCredentialProvider(custom).apply(opts)

	token, _, err := opts.credentialProvider.GetToken(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "custom", token)
}
