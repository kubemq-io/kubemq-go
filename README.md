# KubeMQ - Client library for Go 
KubeMQ is an enterprise grade message broker for containers, designed for any type of workload and architecture running in Kubernetes.
This library is Go implementation of KubeMQ client connection. 
### Installation
`$ go get -u github.com/kubemq-io/go
`
### Examples
Please find usage examples on the examples folders.

### KubeMQ server
Please visit https://kubemq.io, create an account, get KubeMQ token and follow the instructions to run the KubeMQ docker container in your environment. 

## Core Concepts
KubeMQ messaging broker has 4 messaging patterns:

- Events - real time pub/sub pattern 
- Events Store - pub/sub with persistence pattern
- Commands - the Command part of CQRS pattern, which send a commands with response for executed or not (with proper error messaging)
- Queries - the Query part of CQRS pattern, which send a query and get response with the relevant query result back

For each one of the patterns we can distinguish between the senders and the receivers. 

For events and events store the KubeMQ supports both rpc and upstream calls.

the data model is almost identical between all the pattern with some data added related to the specific patter.

The common part of all the patters are:

- Id - the sender can set the Id for each type of message, or the Id will be generate UUID Id for him.
- Metadata - a string field that can hold any metadata related to the message
- Body - a Bytes array which hold the actual payload to be send from the sender to the receiver

The KubeMQ core transport is based on gRPC and the library is a wrapper around the client side of gRPC complied protobuf hence leveraging the gRPC benefits and advantages.

Before any transactions to be perform with KubeMQ server the Client should connect and dial KubeMQ server and obtain Client connection.

With the Client connection object the user can perform all transactions to and from KubeMQ server.

A Client connection object is thread safe and can be shared between all process needed to communicate with KubeMQ.

**IMPORTANT** - it's the user responsibility to close the Client connection when no further communication with KubeMQ is needed.

## Connection

Connecting to KubeMQ server can be done like that:
```
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-event-client-id"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
```
List of connection options:

- WithAddress - set host and port address of KubeMQ server
- WithCredentials - set secured TLS credentials from the input certificate file for client.
- WithToken - set KubeMQ token to be used for KubeMQ connection - not mandatory, only if enforced by the KubeMQ server
- WithClientId - set client id to be used in all functions call with this client - mandatory
- WithReceiveBufferSize - set length of buffered channel to be set in all subscriptions
- WithDefaultChannel - set default channel for any outbound requests
- WithDefaultCacheTTL - set default cache time to live for any query requests with any CacheKey set value

## Events
### Sending Events
#### Single Event
```
err := client.E().
		SetId("some-id").
		SetChannel(channel).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending single event")).
		Send(ctx)
if err != nil {
	log.Fatal(err)
}
```
#### Stream Events
```
		eventStreamCh := make(chan *kubemq.Event, 1)
		errStreamCh := make(chan error, 1)
		go client.StreamEvents(ctx, eventStreamCh, errStreamCh)
		event := client.E().SetId("some-event-id").
			SetChannel("some_channel").
			SetMetadata("some-metadata").
			SetBody([]byte("hello kubemq - sending stream event"))
		for {
			select {
			case err := <-errStreamCh:
				log.Println(err)
				return
			case eventStreamCh <- event:
				return
			}
		}
```
### Receiving Events
First you should subscribe to Events and get a channel:
```
   	channelName := "testing_event_channel"
   	errCh := make(chan error)
   	eventsCh, err := client.SubscribeToEvents(ctx, channelName, "", errCh)
   	if err != nil {
   		log.Fatal(err)
   	}
```
Then you can loop over the channel of events:
```
	for {
		select {
		case err := <-errCh:
			log.Fatal(err)
		case event := <-eventsCh:
			log.Printf("Event Recevied:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
		}
	}
```