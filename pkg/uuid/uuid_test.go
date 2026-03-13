package uuid

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_Format(t *testing.T) {
	id := New()
	require.NotEmpty(t, id)
	parts := strings.Split(id, "-")
	require.Len(t, parts, 5)
	assert.Len(t, parts[0], 8)
	assert.Len(t, parts[1], 4)
	assert.Len(t, parts[2], 4)
	assert.Len(t, parts[3], 4)
	assert.Len(t, parts[4], 12)
}

func TestNew_Unique(t *testing.T) {
	ids := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := New()
		assert.False(t, ids[id], "duplicate UUID generated")
		ids[id] = true
	}
}

func TestNew_Version4(t *testing.T) {
	id := New()
	assert.Equal(t, byte('4'), id[14])
}

func TestUUID_SetVersion(t *testing.T) {
	var u UUID
	u.setVersion(4)
	assert.Equal(t, byte(0x40), u[6]&0xf0)
}

func TestUUID_SetVariant(t *testing.T) {
	var u UUID
	u.setVariant()
	assert.Equal(t, byte(0x80), u[8]&0xc0)
}
