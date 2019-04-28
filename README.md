# KubeMQ - Client library for Go 
KubeMQ is an enterprise grade message broker for containers, designed for any type of workload and architecture running in Kubernetes.
This library is Go implementation of KubeMQ client connection. 
### Installation
`$ go get -u github.com/kubemq-io/kubemq-go
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
- WithTransportType - set client transport type, currently gRPC or Rest

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
		log.Printf("Event Received:\nEventID: %s\nChannel: %s\nMetadata: %s\nBody: %s\n", event.Id, event.Channel, event.Metadata, event.Body)
	}
}
```

## Events Store
### Sending Events Store
#### Single Event to Store
```
//sending 10 single events to store
for i := 0; i < 10; i++ {
	result, err := client.ES().
	    SetId(fmt.Sprintf("event-store-%d", i)).
		SetChannel(channelName).
		SetMetadata("some-metadata").
		SetBody([]byte("hello kubemq - sending single event to store")).
		Send(ctx)
	if err != nil {
			log.Fatal(err)
	}
	log.Printf("Sending event #%d: Result: %t", i, result.Sent)
}
```
#### Stream Events Store
```
// sending addtional events to store
eventsStoreStreamCh := make(chan *kubemq.EventStore, 1)
eventsStoreSResultCh := make(chan *kubemq.EventStoreResult, 1)
errStreamCh := make(chan error, 1)
go client.StreamEventsStore(ctx, eventsStoreStreamCh, eventsStoreSResultCh, errStreamCh)
for i := 0; i < 10; i++ {
    event := client.ES().
    SetId(fmt.Sprintf("event-store-%d", i)).
    SetChannel(channelName).
    SetMetadata("some-metadata").
    SetBody([]byte("hello kubemq - sending stream event to store"))
    eventsStoreStreamCh <- event
    select {
        case err := <-errStreamCh:
    		log.Println(err)
    		return
   		case result := <-eventsStoreSResultCh:
   			log.Printf("Sending event #%d: Result: %t", i, result.Sent)
  		}
}
```
### Receiving Events Store
First you should subscribe to Events Store and get a channel:
```
eventsCh, err := client.SubscribeToEventsStore(ctx, channelName, "", errCh, kubemq.StartFromFirstEvent())
if err != nil {
 	log.Fatal(err)
 }
  
```
#### Subscription Options
KubeMQ supports 6 type of subscriptions:
- StartFromNewEvents - start event store subscription with only new events
- StartFromFirstEvent - replay all the stored events from the first available sequence and continue stream new events from this point
- StartFromLastEvent - replay last event and continue stream new events from this point
- StartFromSequence - replay events from specific event sequence number and continue stream new events from this point
- StartFromTime - replay events from specific time continue stream new events from this point
- StartFromTimeDelta - replay events from specific current time - delta duration in seconds, continue stream new events from this point

Then you can loop over the channel of events:
```
for {
	select {
	case err := <-errCh:
		log.Fatal(err)
	case event := <-eventsCh:
		log.Printf("Receive EventStore\nSequence: %d\nTime: %s\nBody: %s\n", event.Sequence, event.Timestamp, event.Body)
	}
}
```

## Commands
### Concept
Commands implements synchronous messaging pattern which the sender send a request and wait for specific amount of time to get a response.

The response can be successful or not. This is the responsibility of the responder to return with the result of the command within the time the sender set in the request.

### Sending Command Requests
In this example, the responder should send his response withing one second, otherwise an error will be return as timout.
``` 
response, err := client.C().
	SetId("some-command-id").
	SetChannel(channelName).
	SetMetadata("some-metadata").
	SetBody([]byte("hello kubemq - sending command, please reply")).
	SetTimeout(time.Second).
	Send(ctx)
if err != nil {
	log.Fatal(err)
}
```

### Receiving Commands Requests
First get a channel of commands:
``` 
errCh := make(chan error)
commandsCh, err := client.SubscribeToCommands(ctx, channelName, "", errCh)
if err != nil {
		log.Fatal(err)
    }
```
Then a loop over the channel will get the requests from the senders.
```	
for {
	select {
	case err := <-errCh:
		log.Fatal(err)
        return
	case command := <-commandsCh:
		log.Printf("Command Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", command.Id, command.Channel, command.Metadata, command.Body)
	case <-ctx.Done():
		return
	}
}
```

### Sending a Command Response
When sending response there are two important things to remember:
- Set the relevant requestId which you response to
- Set the ResponseTo string to the value of the request ResponseTo field

``` 
err := client.R().
    SetRequestId(command.Id).
	SetResponseTo(command.ResponseTo).
	SetExecutedAt(time.Now()).
	Send(ctx)
if err != nil {
	log.Fatal(err)
}
```

## Queries
### Concept
Queries implements synchronous messaging pattern which the sender send a request and wait for specific amount of time to get a response.

The response must includes metadata or body together with indication of successful or not operation. This is the responsibility of the responder to return with the result of the query within the time the sender set in the request.

### Sending Query Requests
In this example, the responder should send his response withing one second, otherwise an error will be return as timout.
``` 
response, err := client.Q().
    SetId("some-query-id").
    SetChannel(channel).
	SetMetadata("some-metadata").
	SetBody([]byte("hello kubemq - sending a query, please reply")).
	SetTimeout(time.Second).
	Send(ctx)
if err != nil {
	log.Fatal(err)
}
```

### Receiving Query Requests
First get a channel of queries:
``` 
errCh := make(chan error)
queriesCh, err := client.SubscribeToQueries(ctx, channelName, "", errCh)
if err != nil {
	log.Fatal(err)
}
```
Then a loop over the channel will get the requests from the senders.
```	
for {
	select {
	case err := <-errCh:
		log.Fatal(err)
		return
	case query := <-queriesCh:
		log.Printf("Query Received:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", query.Id, query.Channel, query.Metadata, query.Body)
	case <-ctx.Done():
		return
   	}
}
```

### Sending a Query Response
When sending response there are two important things to remember:
- Set the relevant requestId which you response to
- Set the ResponseTo string to the value of the request ResponseTo field

``` 
err := client.R().
    SetRequestId(query.Id).
  	SetResponseTo(query.ResponseTo).
  	SetExecutedAt(time.Now()).
  	SetMetadata("this is a response").
  	SetBody([]byte("got your query, you are good to go")).
  	Send(ctx)
  	
if err != nil {
	log.Fatal(err)
}
```

## Support
- Slack - https://kubemq.slack.com
- Email - support@kubemq.io