package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionInRange(t *testing.T) {
	assert.True(t, isVersionInRange("2.4.1", "2.2.0", "2.99.99"))
	assert.True(t, isVersionInRange("2.2.0", "2.2.0", "2.99.99"))
	assert.True(t, isVersionInRange("v2.4.0", "2.2.0", "2.99.99"))
}

func TestVersionBelowMin(t *testing.T) {
	assert.False(t, isVersionInRange("2.1.0", "2.2.0", "2.99.99"))
	assert.False(t, isVersionInRange("1.9.0", "2.2.0", "2.99.99"))
}

func TestVersionUnparseable(t *testing.T) {
	assert.True(t, isVersionInRange("unknown", "2.2.0", "2.99.99"))
	assert.True(t, isVersionInRange("", "2.2.0", "2.99.99"))
}

func TestParseSimpleVersion_PatchSuffix(t *testing.T) {
	v := parseSimpleVersion("2.4.1-beta")
	require.NotNil(t, v)
	assert.Equal(t, 2, v.major)
	assert.Equal(t, 4, v.minor)
	assert.Equal(t, 1, v.patch)
}
