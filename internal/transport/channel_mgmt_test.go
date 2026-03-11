package transport

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseChannelList_Empty(t *testing.T) {
	result := parseChannelList(nil)
	assert.Nil(t, result)
}

func TestParseChannelList_EmptyBytes(t *testing.T) {
	result := parseChannelList([]byte{})
	assert.Nil(t, result)
}

func TestParseChannelList_InvalidJSON(t *testing.T) {
	result := parseChannelList([]byte("not json"))
	assert.Nil(t, result)
}

func TestParseChannelList_Valid(t *testing.T) {
	channels := []channelListItem{
		{
			Name:         "ch1",
			Type:         "events",
			LastActivity: 1000,
			IsActive:     true,
			Incoming: &channelStat{
				Messages: 100,
				Volume:   5000,
			},
			Outgoing: &channelStat{
				Messages: 50,
				Volume:   2500,
			},
		},
		{
			Name:     "ch2",
			Type:     "queues",
			IsActive: false,
		},
	}

	data, err := json.Marshal(channels)
	require.NoError(t, err)

	result := parseChannelList(data)
	require.Len(t, result, 2)

	assert.Equal(t, "ch1", result[0].Name)
	assert.Equal(t, "events", result[0].Type)
	assert.True(t, result[0].IsActive)
	assert.NotNil(t, result[0].Incoming)
	assert.Equal(t, int64(100), result[0].Incoming.Messages)
	assert.NotNil(t, result[0].Outgoing)
	assert.Equal(t, int64(50), result[0].Outgoing.Messages)

	assert.Equal(t, "ch2", result[1].Name)
	assert.False(t, result[1].IsActive)
	assert.Nil(t, result[1].Incoming)
	assert.Nil(t, result[1].Outgoing)
}

func TestParseChannelList_EmptyArray(t *testing.T) {
	data := []byte("[]")
	result := parseChannelList(data)
	assert.Empty(t, result)
}
