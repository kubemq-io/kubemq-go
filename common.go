package kubemq

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/kubemq-io/kubemq-go/common"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"time"
)

const requestChannel = "kubemq.cluster.internal.requests"

func CreateChannel(ctx context.Context, client *Client, clientId string, channel string, channelType string) error {
	request := NewQuery().
		SetChannel(requestChannel).
		SetId(uuid.New()).
		SetClientId(clientId).
		SetMetadata("create-channel").
		SetTags(map[string]string{
			"channel_type": channelType,
			"channel":      channel,
			"client_id":    clientId,
		}).
		SetTimeout(time.Second * 10)
	resp, err := client.SetQuery(request).Send(ctx)
	if err != nil {
		return fmt.Errorf("error sending create channel request: %s", err.Error())
	}
	if resp.Error != "" {
		return fmt.Errorf("error creating channel: %s", resp.Error)
	}

	return nil
}

func DeleteChannel(ctx context.Context, client *Client, clientId string, channel string, channelType string) error {
	request := NewQuery().
		SetChannel(requestChannel).
		SetId(uuid.New()).
		SetClientId(clientId).
		SetMetadata("delete-channel").
		SetTags(map[string]string{
			"channel_type": channelType,
			"channel":      channel,
			"client_id":    clientId,
		}).
		SetTimeout(time.Second * 10)
	resp, err := client.SetQuery(request).Send(ctx)
	if err != nil {
		return fmt.Errorf("error sending delete channel request: %s", err.Error())
	}
	if resp.Error != "" {
		return fmt.Errorf("error deleting channel: %s", resp.Error)
	}
	return nil
}

func listChannels(ctx context.Context, client *Client, clientId string, channelType string, search string) ([]byte, error) {
	request := NewQuery().
		SetChannel(requestChannel).
		SetId(uuid.New()).
		SetClientId(clientId).
		SetMetadata("list-channels").
		SetTags(map[string]string{
			"channel_type": channelType,
			"client_id":    clientId,
			"search":       search,
		}).
		SetTimeout(time.Second * 10)
	resp, err := client.SetQuery(request).Send(ctx)
	if err != nil {
		return nil, fmt.Errorf("error sending list channels request: %s", err.Error())
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("error listing channels: %s", resp.Error)
	}
	return resp.Body, nil
}
func ListQueuesChannels(ctx context.Context, client *Client, clientId string, search string) ([]*common.QueuesChannel, error) {
	data, err := listChannels(ctx, client, clientId, "queues", search)
	if err != nil {
		return nil, err

	}
	return DecodeQueuesChannelList(data)
}

func ListPubSubChannels(ctx context.Context, client *Client, clientId string, channelType string, search string) ([]*common.PubSubChannel, error) {
	data, err := listChannels(ctx, client, clientId, channelType, search)
	if err != nil {
		return nil, err

	}
	return DecodePubSubChannelList(data)
}

func ListCQChannels(ctx context.Context, client *Client, clientId string, channelType string, search string) ([]*common.CQChannel, error) {
	data, err := listChannels(ctx, client, clientId, channelType, search)
	if err != nil {
		return nil, err
	}
	return DecodeCQChannelList(data)
}

func DecodePubSubChannelList(dataBytes []byte) ([]*common.PubSubChannel, error) {
	var channelsData []*common.PubSubChannel
	if dataBytes == nil {
		return nil, nil
	}
	err := json.Unmarshal(dataBytes, &channelsData)
	if err != nil {
		return nil, err
	}
	return channelsData, nil
}

func DecodeQueuesChannelList(dataBytes []byte) ([]*common.QueuesChannel, error) {
	var channelsData []*common.QueuesChannel
	if dataBytes == nil {
		return nil, nil
	}
	err := json.Unmarshal(dataBytes, &channelsData)
	if err != nil {
		return nil, err
	}
	return channelsData, nil
}

func DecodeCQChannelList(dataBytes []byte) ([]*common.CQChannel, error) {
	var channelsData []*common.CQChannel
	if dataBytes == nil {
		return nil, nil
	}
	err := json.Unmarshal(dataBytes, &channelsData)
	if err != nil {
		return nil, err
	}
	return channelsData, nil
}
