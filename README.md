# KubeMQ Go SDK

The **KubeMQ SDK for Go** enables Go developers to seamlessly communicate with the [KubeMQ](https://kubemq.io/) server, implementing various communication patterns such as Events, EventStore, Commands, Queries, and Queues.
## Table of Contents
<!-- TOC -->
* [KubeMQ Go SDK](#kubemq-go-sdk)
  * [Table of Contents](#table-of-contents)
  * [Prerequisites](#prerequisites)
  * [Installation](#installation)
  * [Running Examples](#running-examples)
  * [SDK Overview](#sdk-overview)
  * [PubSub Events Operations](#pubsub-events-operations)
    * [Create Channel](#create-channel)
      * [Request Parameters](#request-parameters)
      * [Response](#response)
      * [Example](#example)
    * [Delete Channel](#delete-channel)
      * [Request Parameters](#request-parameters-1)
      * [Response](#response-1)
      * [Example](#example-1)
    * [List Channels](#list-channels)
      * [Request Parameters](#request-parameters-2)
      * [Response](#response-2)
      * [Example](#example-2)
    * [Send Event / Subscribe Message](#send-event--subscribe-message)
      * [Send Request: `Event`](#send-request-event)
      * [Send Response](#send-response)
      * [Subscribe Request: `EventsSubscription`](#subscribe-request-eventssubscription)
      * [Example](#example-3)
  * [PubSub EventsStore Operations](#pubsub-eventsstore-operations)
    * [Create Channel](#create-channel-1)
      * [Request Parameters](#request-parameters-3)
      * [Response](#response-3)
      * [Example](#example-4)
    * [Delete Channel](#delete-channel-1)
      * [Request Parameters](#request-parameters-4)
      * [Response](#response-4)
      * [Example](#example-5)
    * [List Channels](#list-channels-1)
      * [Request Parameters](#request-parameters-5)
      * [Response](#response-5)
      * [Example](#example-6)
    * [Send Event / Subscribe Message](#send-event--subscribe-message-1)
      * [Send Request: `Event`](#send-request-event-1)
      * [Send Response](#send-response-1)
      * [Subscribe Request: `EventsStoreSubscription`](#subscribe-request-eventsstoresubscription)
      * [EventsStoreType Options](#eventsstoretype-options)
      * [Example](#example-7)
  * [Commands & Queries – Commands Operations](#commands--queries--commands-operations)
    * [Create Channel](#create-channel-2)
      * [Request Parameters](#request-parameters-6)
      * [Response](#response-6)
      * [Example](#example-8)
    * [Delete Channel](#delete-channel-2)
      * [Request Parameters](#request-parameters-7)
      * [Response](#response-7)
      * [Example](#example-9)
    * [List Channels](#list-channels-2)
      * [Request Parameters](#request-parameters-8)
      * [Response](#response-8)
      * [Example](#example-10)
    * [Send Command / Receive Request](#send-command--receive-request)
      * [Send Request: `CommandMessage`](#send-request-commandmessage-)
      * [Send Response: `CommandResponseMessage`](#send-response-commandresponsemessage-)
      * [Receive Request: `CommandsSubscription`](#receive-request-commandssubscription)
      * [Example](#example-11)
  * [Commands & Queries – Queries Operations](#commands--queries--queries-operations)
    * [Create Channel](#create-channel-3)
      * [Request Parameters](#request-parameters-9)
      * [Response](#response-9)
      * [Example](#example-12)
    * [Delete Channel](#delete-channel-3)
      * [Request Parameters](#request-parameters-10)
      * [Response](#response-10)
      * [Example](#example-13)
    * [List Channels](#list-channels-3)
      * [Request Parameters](#request-parameters-11)
      * [Response](#response-11)
      * [Example](#example-14)
    * [Send Query / Receive Request](#send-query--receive-request)
      * [Send Request: `QueryMessage`](#send-request-querymessage)
      * [Send Response: `QueryResponse`](#send-response-queryresponse)
      * [Receive Request: `QuerySubscription`](#receive-request-querysubscription)
      * [Example](#example-15)
  * [Queues Operations](#queues-operations)
    * [Create Channel](#create-channel-4)
      * [Request Parameters](#request-parameters-12)
      * [Response](#response-12)
      * [Example](#example-16)
    * [Delete Channel](#delete-channel-4)
      * [Request Parameters](#request-parameters-13)
      * [Response](#response-13)
      * [Example](#example-17)
    * [List Channels](#list-channels-4)
      * [Request Parameters](#request-parameters-14)
      * [Response](#response-14)
      * [Example](#example-18)
    * [Send / Receive Queue Messages](#send--receive-queue-messages)
      * [Send Request: `QueueMessage`](#send-request-queuemessage-)
      * [Send Response: `SendResult`](#send-response-sendresult-)
      * [Receive Request: `PollRequest`](#receive-request-pollrequest)
      * [Response: `PollResponse`](#response-pollresponse)
        * [Response: `QueueMessage`](#response-queuemessage-)
      * [Example](#example-19)
      * [Example with Visibility](#example-with-visibility)
<!-- TOC -->

## Prerequisites

- Go SDK 1.17 higher
- KubeMQ server running locally or accessible over the network


## Installation
```bash
go get github.com/kubemq-io/kubemq-go
```

## Running Examples

The [examples](https://github.com/kubemq-io/kubemq-go/tree/master/examples) are standalone projects that showcase the usage of the SDK. To run the examples, ensure you have a running instance of KubeMQ.

## SDK Overview

The SDK implements all communication patterns available through the KubeMQ server:
- PubSub
    - Events
    - EventStore
- Commands & Queries (CQ)
    - Commands
    - Queries
- Queues

## PubSub Events Operations

### Create Channel

Create a new Events channel.

#### Request Parameters

| Name        | Type    | Description                             | Default Value | Mandatory |
|-------------|---------|-----------------------------------------|---------------|-----------|
| Ctx         | context | The context for the request.            | None          | Yes       |
| ChannelName | String  | Name of the channel you want to create  | None          | Yes       |

#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func createEventsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-creator"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsClient.Create(ctx, "events-channel"); err != nil {
		log.Fatal(err)
	}
}

```

### Delete Channel

Delete an existing Events channel.

#### Request Parameters

| Name        | Type   | Description                             | Default Value | Mandatory |
|-------------|--------|-----------------------------------------|---------------|-----------|
| ChannelName | String | Name of the channel you want to delete  | None          | Yes       |


#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func deleteEventsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-delete"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsClient.Delete(ctx, "events-channel"); err != nil {
		log.Fatal(err)
	}
}
```

### List Channels

Retrieve a list of Events channels.

#### Request Parameters

| Name        | Type   | Description                                | Default Value | Mandatory |
|-------------|--------|--------------------------------------------|---------------|-----------|
| SearchQuery | String | Search query to filter channels (optional) | None          | No        |

#### Response

Returns a list where each `PubSubChannel` has the following attributes:

| Name         | Type        | Description                                                                                   |
|--------------|-------------|-----------------------------------------------------------------------------------------------|
| Name         | String      | The name of the Pub/Sub channel.                                                              |
| Type         | String      | The type of the Pub/Sub channel.                                                              |
| LastActivity | long        | The timestamp of the last activity on the channel, represented in milliseconds since epoch.   |
| IsActive     | boolean     | Indicates whether the channel is active or not.                                               |
| Incoming     | PubSubStats | The statistics related to incoming messages for this channel.                                 |
| Outgoing     | PubSubStats | The statistics related to outgoing messages for this channel.                                 |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listEventsChannels() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-channel-lister"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := eventsClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}
```

### Send Event / Subscribe Message

Sends a message to an Events channel.

#### Send Request: `Event`

| Name     | Type                | Description                                                | Default Value    | Mandatory |
|----------|---------------------|------------------------------------------------------------|------------------|-----------|
| Id       | String              | Unique identifier for the event message.                   | None             | No        |
| Channel  | String              | The channel to which the event message is sent.            | None             | Yes       |
| Metadata | String              | Metadata associated with the event message.                | None             | No        |
| Body     | byte[]              | Body of the event message in bytes.                        | Empty byte array | No        |
| Tags     | Map<String, String> | Tags associated with the event message as key-value pairs. | Empty Map        | No        |


#### Send Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Subscribe Request: `EventsSubscription`

| Name                   | Type                           | Description                                                          | Default Value | Mandatory |
|------------------------|--------------------------------|----------------------------------------------------------------------|---------------|-----------|
| Channel                | String                         | The channel to subscribe to.                                         | None          | Yes       |
| Group                  | String                         | The group to subscribe with.                                         | None          | No        |


#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendSubscribeEvents() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsClient, err := kubemq.NewEventsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-send-subscribe"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subReq := &kubemq.EventsSubscription{
		Channel:  "events-channel",
		Group:    "",
		ClientId: "",
	}
	err = eventsClient.Subscribe(ctx, subReq, func(msg *kubemq.Event, err error) {
		log.Println(msg.String())
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(300 * time.Second)

	err = eventsClient.Send(ctx, &kubemq.Event{
		Channel:  "events-channel",
		Metadata: "some-metadata",
		Body:     []byte("hello kubemq - sending event"),
	})

	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
}
```

## PubSub EventsStore Operations

### Create Channel

Create a new Events Store channel.

#### Request Parameters

| Name        | Type    | Description                             | Default Value | Mandatory |
|-------------|---------|-----------------------------------------|---------------|-----------|
| Ctx         | context | The context for the request.            | None          | Yes       |
| ChannelName | String  | Name of the channel you want to create  | None          | Yes       |

#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |


#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)
func createEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-creator"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsStoreClient.Create(ctx, "events-store-channel"); err != nil {
		log.Fatal(err)
	}
}
```
### Delete Channel

Delete an existing Events Store channel.

#### Request Parameters

| Name        | Type   | Description                             | Default Value | Mandatory |
|-------------|--------|-----------------------------------------|---------------|-----------|
| ChannelName | String | Name of the channel you want to delete  | None          | Yes       |


#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func deleteEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-delete"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := eventsStoreClient.Delete(ctx, "events-store-channel"); err != nil {
		log.Fatal(err)
	}
}
```

### List Channels

Retrieve a list of Events channels.

#### Request Parameters

| Name        | Type   | Description                                | Default Value | Mandatory |
|-------------|--------|--------------------------------------------|---------------|-----------|
| SearchQuery | String | Search query to filter channels (optional) | None          | No        |

#### Response

Returns a list where each `PubSubChannel` has the following attributes:

| Name         | Type        | Description                                                                                   |
|--------------|-------------|-----------------------------------------------------------------------------------------------|
| Name         | String      | The name of the Pub/Sub channel.                                                              |
| Type         | String      | The type of the Pub/Sub channel.                                                              |
| LastActivity | long        | The timestamp of the last activity on the channel, represented in milliseconds since epoch.   |
| IsActive     | boolean     | Indicates whether the channel is active or not.                                               |
| Incoming     | PubSubStats | The statistics related to incoming messages for this channel.                                 |
| Outgoing     | PubSubStats | The statistics related to outgoing messages for this channel.                                 |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listEventsStoreChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-channel-lister"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := eventsStoreClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}
```


### Send Event / Subscribe Message

Sends a message to an Events channel.

#### Send Request: `Event`

| Name     | Type                | Description                                                | Default Value    | Mandatory |
|----------|---------------------|------------------------------------------------------------|------------------|-----------|
| Id       | String              | Unique identifier for the event message.                   | None             | No        |
| Channel  | String              | The channel to which the event message is sent.            | None             | Yes       |
| Metadata | String              | Metadata associated with the event message.                | None             | No        |
| Body     | byte[]              | Body of the event message in bytes.                        | Empty byte array | No        |
| Tags     | Map<String, String> | Tags associated with the event message as key-value pairs. | Empty Map        | No        |


#### Send Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Subscribe Request: `EventsStoreSubscription`

| Name                   | Type                           | Description                  | Default Value | Mandatory |
|------------------------|--------------------------------|------------------------------|---------------|-----------|
| Channel                | String                         | The channel to subscribe to. | None          | Yes       |
| Group                  | String                         | The group to subscribe with. | None          | No        |
| SubscriptionType                  | EventsStoreSubscription                         | The Subscription             | None          | Yes       |


#### EventsStoreType Options

| Type              | Value | Description                                                        |
|-------------------|-------|--------------------------------------------------------------------|
| StartNewOnly      | 1     | Start storing events from the point when the subscription is made  |
| StartFromFirst    | 2     | Start storing events from the first event available                |
| StartFromLast     | 3     | Start storing events from the last event available                 |
| StartAtSequence   | 4     | Start storing events from a specific sequence number               |
| StartAtTime       | 5     | Start storing events from a specific point in time                 |
| StartAtTimeDelta  | 6     | Start storing events from a specific time delta in seconds         |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendSubscribeEventsStore() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsStoreClient, err := kubemq.NewEventsStoreClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("events-store-send-subscribe"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := eventsStoreClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subReq := &kubemq.EventsStoreSubscription{
		Channel:          "events-store-channel",
		Group:            "",
		ClientId:         "",
		SubscriptionType: kubemq.StartFromFirstEvent(),
	}
	err = eventsStoreClient.Subscribe(ctx, subReq, func(msg *kubemq.EventStoreReceive, err error) {
		log.Println(msg.String())
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	result, err := eventsStoreClient.Send(ctx, &kubemq.EventStore{
		Channel:  "events-store-channel",
		Metadata: "some-metadata",
		Body:     []byte("hello kubemq - sending event store"),
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	time.Sleep(1 * time.Second)

}
```



## Commands & Queries – Commands Operations

### Create Channel

Create a new Command channel.

#### Request Parameters

| Name        | Type    | Description                             | Default Value | Mandatory |
|-------------|---------|-----------------------------------------|---------------|-----------|
| Ctx         | context | The context for the request.            | None          | Yes       |
| ChannelName | String  | Name of the channel you want to create  | None          | Yes       |

#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func createCommandsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := commandsClient.Create(ctx, "commands.A"); err != nil {
		log.Fatal(err)
	}
}

```

### Delete Channel

Delete an existing Command channel.

#### Request Parameters

| Name        | Type   | Description                             | Default Value | Mandatory |
|-------------|--------|-----------------------------------------|---------------|-----------|
| ChannelName | String | Name of the channel you want to delete  | None          | Yes       |


#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |


#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func deleteCommandsChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := commandsClient.Delete(ctx, "commands.A"); err != nil {
		log.Fatal(err)
	}
}
```

### List Channels

Retrieve a list of Command channels.


#### Request Parameters

| Name        | Type   | Description                                | Default Value | Mandatory |
|-------------|--------|--------------------------------------------|---------------|-----------|
| SearchQuery | String | Search query to filter channels (optional) | None          | No        |

#### Response

Returns a list where each `CQChannel` has the following attributes:

| Name         | Type        | Description                                                                                   |
|--------------|-------------|-----------------------------------------------------------------------------------------------|
| Name         | String      | The name of the Pub/Sub channel.                                                              |
| Type         | String      | The type of the Pub/Sub channel.                                                              |
| LastActivity | long        | The timestamp of the last activity on the channel, represented in milliseconds since epoch.   |
| IsActive     | boolean     | Indicates whether the channel is active or not.                                               |
| Incoming     | PubSubStats | The statistics related to incoming messages for this channel.                                 |
| Outgoing     | PubSubStats | The statistics related to outgoing messages for this channel.                                 |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listCommandsChannels() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := commandsClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}
```

### Send Command / Receive Request

Sends a command request to a Command channel.

#### Send Request: `CommandMessage` 

| Name           | Type                | Description                                                                            | Default Value     | Mandatory |
|----------------|---------------------|----------------------------------------------------------------------------------------|-------------------|-----------|
| Id             | String              | The ID of the command message.                                                         | None              | Yes       |
| Channel        | String              | The channel through which the command message will be sent.                            | None          | Yes       |
| Metadata       | String              | Additional metadata associated with the command message.                               | None             | No        |
| Body           | byte[]              | The body of the command message as bytes.                                              | Empty byte array  | No        |
| Tags           | Map<String, String> | A dictionary of key-value pairs representing tags associated with the command message. | Empty Map | No |
| Timeout | Duration            | The maximum time duration for waiting to response.                          | None    | Yes       |

#### Send Response: `CommandResponseMessage` 

| Name             | Type    | Description                                          |
|------------------|---------|------------------------------------------------------|
| CommandId        | String  | Command Id                                           |
| ResponseClientId | String  | The client ID associated with the command response.  |
| Executed         | boolean | Indicates if the command has been executed.          |
| ExecutedAt       | time    | The timestamp when the command response was created. |
| Error            | String  | The error message if there was an error.             |

#### Receive Request: `CommandsSubscription`


| Name                     | Type                             | Description                                   | Default Value | Mandatory |
|--------------------------|----------------------------------|-----------------------------------------------|---------------|-----------|
| Channel                  | String                           | The channel for the subscription.             | None          | Yes       |
| Group                   | String                           | The group associated with the subscription.   | None          | No        |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendReceiveCommands() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandsClient, err := kubemq.NewCommandsClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("sendReceiveCommands"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := commandsClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subRequest := &kubemq.CommandsSubscription{
		Channel:  "commands",
		ClientId: "",
		Group:    "",
	}
	log.Println("subscribing to commands")
	err = commandsClient.Subscribe(ctx, subRequest, func(cmd *kubemq.CommandReceive, err error) {
		log.Println(cmd.String())
		resp := &kubemq.Response{
			RequestId:  cmd.Id,
			ResponseTo: cmd.ResponseTo,
			Metadata:   "some-metadata",
			ExecutedAt: time.Now(),
		}
		if err := commandsClient.Response(ctx, resp); err != nil {
			log.Fatal(err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	log.Println("sending command")
	result, err := commandsClient.Send(ctx, kubemq.NewCommand().
		SetChannel("commands").
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending command")).
		SetTimeout(time.Duration(10)*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}
```

## Commands & Queries – Queries Operations

### Create Channel

Create a new Query channel.

#### Request Parameters

| Name        | Type    | Description                             | Default Value | Mandatory |
|-------------|---------|-----------------------------------------|---------------|-----------|
| Ctx         | context | The context for the request.            | None          | Yes       |
| ChannelName | String  | Name of the channel you want to create  | None          | Yes       |

#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)


func createQueriesChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queriesClient.Create(ctx, "queries.A"); err != nil {
		log.Fatal(err)
	}
}

```

### Delete Channel

Delete an existing Query channel.

#### Request Parameters

| Name        | Type   | Description                             | Default Value | Mandatory |
|-------------|--------|-----------------------------------------|---------------|-----------|
| ChannelName | String | Name of the channel you want to delete  | None          | Yes       |


#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |


#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func deleteQueriesChannel() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queriesClient.Delete(ctx, "queries.A"); err != nil {
		log.Fatal(err)
	}
}
```

### List Channels

Retrieve a list of Query channels.


#### Request Parameters

| Name        | Type   | Description                                | Default Value | Mandatory |
|-------------|--------|--------------------------------------------|---------------|-----------|
| SearchQuery | String | Search query to filter channels (optional) | None          | No        |

#### Response

Returns a list where each `CQChannel` has the following attributes:

| Name         | Type        | Description                                                                                   |
|--------------|-------------|-----------------------------------------------------------------------------------------------|
| Name         | String      | The name of the Pub/Sub channel.                                                              |
| Type         | String      | The type of the Pub/Sub channel.                                                              |
| LastActivity | long        | The timestamp of the last activity on the channel, represented in milliseconds since epoch.   |
| IsActive     | boolean     | Indicates whether the channel is active or not.                                               |
| Incoming     | PubSubStats | The statistics related to incoming messages for this channel.                                 |
| Outgoing     | PubSubStats | The statistics related to outgoing messages for this channel.                                 |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
)

func listQueriesChannels() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := queriesClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}
```

### Send Query / Receive Request

Sends a query request to a Query channel.

#### Send Request: `QueryMessage`

| Name           | Type                | Description                                                                            | Default Value     | Mandatory |
|----------------|---------------------|----------------------------------------------------------------------------------------|-------------------|-----------|
| Id             | String              | The ID of the query message.                                                           | None              | Yes       |
| Channel        | String              | The channel through which the query message will be sent.                            | None          | Yes       |
| Metadata       | String              | Additional metadata associated with the query message.                               | None             | No        |
| Body           | byte[]              | The body of the query message as bytes.                                              | Empty byte array  | No        |
| Tags           | Map<String, String> | A dictionary of key-value pairs representing tags associated with the query message. | Empty Map | No |
| Timeout | Duration            | The maximum time duration for waiting to response.                                     | None    | Yes       |

#### Send Response: `QueryResponse`

| Name             | Type                | Description                                                                                   |
|------------------|---------------------|-----------------------------------------------------------------------------------------------|
| QueryId          | String              | Query Id                                                                                      |
| ResponseClientId | String              | The client ID associated with the query response.                                             |
| Executed         | boolean             | Indicates if the query has been executed.                                                     |
| Metadata         | String              | Additional metadata associated with the query response message.                               | None             | No        |
| Body             | byte[]              | The body of the query response message as bytes.                                              | Empty byte array  | No        |
| Tags             | Map<String, String> | A dictionary of key-value pairs representing tags associated with the query response message. | Empty Map | No |
| ExecutedAt       | time                | The timestamp when the query response was created.                                            |
| Error            | String              | The error message if there was an error.                                                      |

#### Receive Request: `QuerySubscription`


| Name                     | Type                             | Description                                   | Default Value | Mandatory |
|--------------------------|----------------------------------|-----------------------------------------------|---------------|-----------|
| Channel                  | String                           | The channel for the subscription.             | None          | Yes       |
| Group                   | String                           | The group associated with the subscription.   | None          | No        |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func sendReceiveQueries() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queriesClient, err := kubemq.NewQueriesClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("sendReceiveQueries"))

	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queriesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	subRequest := &kubemq.QueriesSubscription{
		Channel:  "queries",
		ClientId: "",
		Group:    "",
	}
	log.Println("subscribing to queries")
	err = queriesClient.Subscribe(ctx, subRequest, func(query *kubemq.QueryReceive, err error) {
		log.Println(query.String())
		resp := &kubemq.Response{
			RequestId:  query.Id,
			ResponseTo: query.ResponseTo,
			Metadata:   "some-metadata",
			ExecutedAt: time.Now(),
			Body:       []byte("hello kubemq - sending query response"),
		}
		if err := queriesClient.Response(ctx, resp); err != nil {
			log.Fatal(err)
		}
	})
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(1 * time.Second)
	log.Println("sending query")
	result, err := queriesClient.Send(ctx, kubemq.NewQuery().
		SetChannel("queries").
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending query")).
		SetTimeout(time.Duration(10)*time.Second))
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
}
```

## Queues Operations


### Create Channel

Create a new Queue channel.

#### Request Parameters

| Name        | Type    | Description                             | Default Value | Mandatory |
|-------------|---------|-----------------------------------------|---------------|-----------|
| Ctx         | context | The context for the request.            | None          | Yes       |
| ChannelName | String  | Name of the channel you want to create  | None          | Yes       |

#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |


#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queuesClient.Create(ctx, "queues.A"); err != nil {
		log.Fatal(err)
	}
}
```
### Delete Channel

Delete an existing Queue channel.

#### Request Parameters

| Name        | Type   | Description                             | Default Value | Mandatory |
|-------------|--------|-----------------------------------------|---------------|-----------|
| ChannelName | String | Name of the channel you want to delete  | None          | Yes       |


#### Response

| Name | Type    | Description                    |
|------|---------|--------------------------------|
| Err  | error   | Error message if any            |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	if err := queuesClient.Delete(ctx, "queues.A"); err != nil {
		log.Fatal(err)
	}
}

```

### List Channels

Retrieve a list of Queue channels.

#### Request Parameters

| Name        | Type   | Description                                | Default Value | Mandatory |
|-------------|--------|--------------------------------------------|---------------|-----------|
| SearchQuery | String | Search query to filter channels (optional) | None          | No        |

#### Response

Returns a list where each `QueuesChannel` has the following attributes:

| Name         | Type        | Description                                                                                   |
|--------------|-------------|-----------------------------------------------------------------------------------------------|
| Name         | String      | The name of the Pub/Sub channel.                                                              |
| Type         | String      | The type of the Pub/Sub channel.                                                              |
| LastActivity | long        | The timestamp of the last activity on the channel, represented in milliseconds since epoch.   |
| IsActive     | boolean     | Indicates whether the channel is active or not.                                               |
| Incoming     | PubSubStats | The statistics related to incoming messages for this channel.                                 |
| Outgoing     | PubSubStats | The statistics related to outgoing messages for this channel.                                 |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	channels, err := queuesClient.List(ctx, "")
	if err != nil {
		log.Fatal(err)
	}
	for _, channel := range channels {
		log.Println(channel)
	}
}
```

### Send / Receive Queue Messages

Send and receive messages from a Queue channel.

#### Send Request: `QueueMessage` 

| Name                    | Type                | Description                                                                                         | Default Value   | Mandatory |
|-------------------------|---------------------|-----------------------------------------------------------------------------------------------------|-----------------|-----------|
| Id                      | String              | The unique identifier for the message.                                                              | None            | No        |
| Channel                 | String              | The channel of the message.                                                                         | None            | Yes       |
| Metadata                | String              | The metadata associated with the message.                                                           | None            | No        |
| Body                    | byte[]              | The body of the message.                                                                            | new byte[0]     | No        |
| Tags                    | Map<String, String> | The tags associated with the message.                                                               | new HashMap<>() | No        |
| PolicyDelaySeconds      | int                 | The delay in seconds before the message becomes available in the queue.                             | None            | No        |
| PolicyExpirationSeconds | int                 | The expiration time in seconds for the message.                                                     | None            | No        |
| PolicyMaxReceiveCount   | int                 | The number of receive attempts allowed for the message before it is moved to the dead letter queue. | None            | No        |
| PolicyMaxReceiveQueue   | String              | The dead letter queue where the message will be moved after reaching the maximum receive attempts.  | None            | No        |

#### Send Response: `SendResult` 

| Name      | Type            | Description                                                   |
|-----------|-----------------|---------------------------------------------------------------|
| Id        | String          | The unique identifier of the message.                         |
| SentAt    | LocalDateTime   | The timestamp when the message was sent.                      |
| ExpiredAt | LocalDateTime   | The timestamp when the message will expire.                   |
| DelayedTo | LocalDateTime   | The timestamp when the message will be delivered.             |
| IsError   | boolean         | Indicates if there was an error while sending the message.    |
| Error     | String          | The error message if `isError` is true.                       |

#### Receive Request: `PollRequest`

| Name              | Type    | Description                                        | Default Value | Mandatory |
|-------------------|---------|----------------------------------------------------|---------------|-----------|
| Channel           | String  | The channel to poll messages from.                 | None          | Yes       |
| MaxItems          | int     | The maximum number of messages to poll.            | 1             | No        |
| WaitTimeout       | int     | The wait timeout in seconds for polling messages.  | 60            | No        |
| AutoAck           | boolean | Indicates if messages should be auto-acknowledged. | false         | No        |
| VisibilitySeconds | int     | Add a visibility timeout feature for messages.     | 0             | No        |

#### Response: `PollResponse`

| Name     | Type                       | Description                                             |
|----------|----------------------------|---------------------------------------------------------|
| Messages | List<QueueMessage> | The list of received queue messages.                    |


##### Response: `QueueMessage` 


| Name                   | Type                                  | Description                                             |
|------------------------|---------------------------------------|---------------------------------------------------------|
| Id                     | String                                | The unique identifier for the message.                  |
| Channel                | String                                | The channel from which the message was received.         |
| Metadata               | String                                | Metadata associated with the message.                   |
| Body                   | byte[]                                | The body of the message in byte array format.           |
| ClientID               | String                                | The ID of the client that sent the message.             |
| Tags                   | Map`<String, String>`                 | Key-value pairs representing tags for the message.      |
| Timestamp              | Instant                               | The timestamp when the message was created.             |
| Sequence               | long                                  | The sequence number of the message.                     |
| ReceiveCount           | int                                   | The number of times the message has been received.       |
| ReRouted             | boolean                               | Indicates whether the message was rerouted.             |
| ReRoutedFromQueue       | String                                | The name of the queue from which the message was rerouted.|
| ExpirationAt              | Instant                               | The expiration time of the message, if applicable.      |
| DelayedTo              | Instant                               | The time the message is delayed until, if applicable.   |

#### Example

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
	"time"
)

func sendAndReceive() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceive").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue")).
		SetTags(map[string]string{
			"key1": "value1",
			"key2": "value2",
		}).
		SetPolicyDelaySeconds(1).
		SetPolicyExpirationSeconds(10).
		SetPolicyMaxReceiveCount(3).
		SetPolicyMaxReceiveQueue("dlq")
	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceive").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetAutoAck(true)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//AckAll - Acknowledge all messages
	//if err := msgs.AckAll(); err != nil {
	//	log.Fatal(err)
	//}

	//NackAll - Not Acknowledge all messages
	//if err := msgs.NAckAll(); err != nil {
	//	log.Fatal(err)
	//}

	// RequeueAll - Requeue all messages
	//if err := msgs.ReQueueAll("requeue-queue-channel"); err != nil {
	//	log.Fatal(err)
	//}

	for _, msg := range msgs.Messages {
		log.Println(msg.String())

		// Ack - Acknowledge message
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}

		// Nack - Not Acknowledge message
		//if err := msg.NAck(); err != nil {
		//	log.Fatal(err)
		//}

		// Requeue - Requeue message
		//if err := msg.ReQueue("requeue-queue-channel"); err != nil {
		//	log.Fatal(err)
		//}
	}

}
```



#### Example with Visibility

```go
package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go/queues_stream"
	"log"
	"time"
)

func sendAndReceiveWithVisibilityExpiration() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceiveWithVisibility").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue - with visibility"))

	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceiveWithVisibility").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetVisibilitySeconds(2)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	for _, msg := range msgs.Messages {
		log.Println(msg.String())
		log.Println("Received message, waiting 3 seconds before ack")
		time.Sleep(3 * time.Second)
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}

}
func sendAndReceiveWithVisibilityExtension() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	queuesClient, err := queues_stream.NewQueuesStreamClient(ctx,
		queues_stream.WithAddress("localhost", 50000),
		queues_stream.WithClientId("example"))
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := queuesClient.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	msg := queues_stream.NewQueueMessage().
		SetId("message_id").
		SetChannel("sendAndReceiveWithVisibility").
		SetMetadata("some-metadata").
		SetBody([]byte("hello world from KubeMQ queue - with visibility"))

	result, err := queuesClient.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(result)
	pollRequest := queues_stream.NewPollRequest().
		SetChannel("sendAndReceiveWithVisibility").
		SetMaxItems(1).
		SetWaitTimeout(10).
		SetVisibilitySeconds(2)
	msgs, err := queuesClient.Poll(ctx, pollRequest)
	for _, msg := range msgs.Messages {
		log.Println(msg.String())
		log.Println("Received message, waiting 1 seconds before ack")
		time.Sleep(1 * time.Second)
		log.Println("Extending visibility for 3 seconds and waiting 2 seconds before ack")
		if err := msg.ExtendVisibility(3); err != nil {
			log.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if err := msg.Ack(); err != nil {
			log.Fatal(err)
		}
	}
}
```
