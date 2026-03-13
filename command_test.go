package kubemq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCommand(t *testing.T) {
	cmd := NewCommand()
	require.NotNil(t, cmd)
	assert.Empty(t, cmd.Id)
	assert.Empty(t, cmd.Channel)
}

func TestCommand_Setters(t *testing.T) {
	cmd := NewCommand().
		SetId("id1").
		SetClientId("client1").
		SetChannel("orders").
		SetMetadata("meta").
		SetBody([]byte("data")).
		SetTimeout(5*time.Second).
		SetTags(map[string]string{"k": "v"}).
		AddTag("k2", "v2")

	assert.Equal(t, "id1", cmd.Id)
	assert.Equal(t, "client1", cmd.ClientId)
	assert.Equal(t, "orders", cmd.Channel)
	assert.Equal(t, "meta", cmd.Metadata)
	assert.Equal(t, []byte("data"), cmd.Body)
	assert.Equal(t, 5*time.Second, cmd.Timeout)
	assert.Equal(t, "v", cmd.Tags["k"])
	assert.Equal(t, "v2", cmd.Tags["k2"])
}

func TestCommand_SetTags_DefensiveCopy(t *testing.T) {
	original := map[string]string{"a": "1"}
	cmd := NewCommand().SetTags(original)
	original["a"] = "modified"
	assert.Equal(t, "1", cmd.Tags["a"])
}

func TestCommand_AddTag_NilInit(t *testing.T) {
	cmd := &Command{}
	cmd.AddTag("key", "val")
	assert.Equal(t, "val", cmd.Tags["key"])
}

func TestCommand_Validate_Valid(t *testing.T) {
	cmd := NewCommand().SetChannel("orders").SetTimeout(5 * time.Second).SetBody([]byte("data"))
	assert.NoError(t, cmd.Validate())
}

func TestCommand_Validate_MissingChannel(t *testing.T) {
	cmd := NewCommand().SetTimeout(5 * time.Second)
	err := cmd.Validate()
	require.Error(t, err)
	var kErr *KubeMQError
	require.ErrorAs(t, err, &kErr)
	assert.Equal(t, ErrCodeValidation, kErr.Code)
}

func TestCommand_Validate_MissingTimeout(t *testing.T) {
	cmd := NewCommand().SetChannel("orders")
	err := cmd.Validate()
	require.Error(t, err)
}

func TestCommandReceive_String(t *testing.T) {
	cr := &CommandReceive{Id: "id1", Channel: "ch", ClientId: "c1", ResponseTo: "resp"}
	s := cr.String()
	assert.Contains(t, s, "id1")
	assert.Contains(t, s, "ch")
	assert.Contains(t, s, "resp")
}

func TestCommandResponse_String(t *testing.T) {
	cr := &CommandResponse{CommandId: "cmd1", Executed: true, Error: ""}
	s := cr.String()
	assert.Contains(t, s, "cmd1")
	assert.Contains(t, s, "true")
}
