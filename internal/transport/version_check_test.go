package transport

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
