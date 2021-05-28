package kubemq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kubemq-io/kubemq-go/pkg/uuid"
	"net/http"

	pb "github.com/kubemq-io/protobuf/go"
	"github.com/nats-io/nuid"

	"github.com/go-resty/resty/v2"
	"github.com/gorilla/websocket"
)

type restResponse struct {
	IsError bool            `json:"is_error"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (res *restResponse) unmarshal(v interface{}) error {
	if res.IsError {
		return errors.New(res.Message)
	}
	if v == nil {
		return nil
	}
	if res.Data == nil {
		return nil
	}
	return json.Unmarshal(res.Data, v)
}

func newWebsocketConn(ctx context.Context, uri string, readCh chan string, ready chan struct{}, errCh chan error, authToken string) (*websocket.Conn, error) {
	var c *websocket.Conn
	header := http.Header{}
	if authToken != "" {
		header.Add("Authorization", fmt.Sprintf("Bearer %s", authToken))
	}
	conn, res, err := websocket.DefaultDialer.Dial(uri, header)
	if err != nil {
		buf := make([]byte, 1024)
		if res != nil {
			n, _ := res.Body.Read(buf)
			return nil, errors.New(string(buf[:n]))
		} else {
			return nil, err
		}
	} else {
		c = conn
	}
	ready <- struct{}{}
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case readCh <- string(message):

			case <-ctx.Done():
				return
			}

		}
	}()
	return c, nil
}

func newBiDirectionalWebsocketConn(ctx context.Context, uri string, readCh chan string, writeCh chan []byte, ready chan struct{}, errCh chan error, authToken string) (*websocket.Conn, error) {
	var c *websocket.Conn
	header := http.Header{}
	if authToken != "" {
		header.Add("Authorization", fmt.Sprintf("Bearer %s", authToken))
	}
	conn, res, err := websocket.DefaultDialer.Dial(uri, header)
	if err != nil {
		buf := make([]byte, 1024)
		if res != nil {
			n, _ := res.Body.Read(buf)
			return nil, errors.New(string(buf[:n]))
		} else {
			return nil, err
		}
	} else {
		c = conn
	}
	ready <- struct{}{}
	go func() {
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case readCh <- string(message):

			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for {

			select {
			case message := <-writeCh:
				err := c.WriteMessage(1, message)
				if err != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return c, nil
}

type restTransport struct {
	id          string
	restAddress string
	wsAddress   string
	wsConn      *websocket.Conn
	opts        *Options
}

func newRestTransport(ctx context.Context, opts *Options) (Transport, *ServerInfo, error) {
	rt := &restTransport{
		id:          nuid.New().Next(),
		restAddress: opts.restUri,
		wsAddress:   opts.webSocketUri,
		wsConn:      nil,
		opts:        opts,
	}
	if rt.opts.checkConnection {
		si, err := rt.Ping(ctx)
		if err != nil {
			return nil, &ServerInfo{}, err
		}
		return rt, si, nil
	} else {
		return rt, &ServerInfo{}, nil
	}
}
func (rt *restTransport) newRequest() *resty.Request {
	r := resty.New().R()
	if rt.opts.authToken != "" {
		r.SetAuthToken(rt.opts.authToken)
	}
	return r

}
func (rt *restTransport) Ping(ctx context.Context) (*ServerInfo, error) {
	resp := &restResponse{}
	uri := fmt.Sprintf("%s/ping", rt.restAddress)
	_, err := rt.newRequest().SetResult(resp).SetError(resp).Get(uri)
	if err != nil {
		return nil, err
	}
	si := &ServerInfo{}
	if err := resp.unmarshal(si); err != nil {
		return nil, err
	}
	return si, nil
}

func (rt *restTransport) SendEvent(ctx context.Context, event *Event) error {
	resp := &restResponse{}
	eventPb := &pb.Event{
		EventID:  event.Id,
		ClientID: event.ClientId,
		Channel:  event.Channel,
		Metadata: event.Metadata,
		Body:     event.Body,
		Store:    false,
		Tags:     event.Tags,
	}
	uri := fmt.Sprintf("%s/send/event", rt.restAddress)
	_, err := rt.newRequest().SetBody(eventPb).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return err
	}
	if err := resp.unmarshal(nil); err != nil {
		return err
	}
	return nil
}

func (rt *restTransport) StreamEvents(ctx context.Context, eventsCh chan *Event, errCh chan error) {
	uri := fmt.Sprintf("%s/send/stream", rt.wsAddress)
	readCh := make(chan string)
	writeCh := make(chan []byte)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := newBiDirectionalWebsocketConn(newCtx, uri, readCh, writeCh, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		errCh <- err
		return
	}
	rt.wsConn = conn
	<-ready

	go func() {
		for {
			select {
			case <-readCh:

			case err := <-wsErrCh:
				errCh <- err
				return
			case <-newCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case event := <-eventsCh:
			data, _ := json.Marshal(event)
			writeCh <- data
		case err := <-wsErrCh:
			errCh <- err
			return
		case <-newCtx.Done():
			return
		}
	}
}

func (rt *restTransport) SubscribeToEvents(ctx context.Context, request *EventsSubscription, errCh chan error) (<-chan *Event, error) {
	eventsCh := make(chan *Event, rt.opts.receiveBufferSize)
	uri := fmt.Sprintf("%s/subscribe/events?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", rt.wsAddress, request.ClientId, request.Channel, request.Group, "events")
	rxChan := make(chan string)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	conn, err := newWebsocketConn(ctx, uri, rxChan, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		return nil, err
	}
	rt.wsConn = conn
	<-ready
	go func() {
		for {
			select {
			case pbMsg := <-rxChan:
				eventReceive := &pb.EventReceive{}
				err := json.Unmarshal([]byte(pbMsg), eventReceive)
				if err != nil {
					errCh <- err
					return
				}
				eventsCh <- &Event{
					Id:        eventReceive.EventID,
					Channel:   eventReceive.Channel,
					Metadata:  eventReceive.Metadata,
					Body:      eventReceive.Body,
					ClientId:  "",
					Tags:      eventReceive.Tags,
					transport: nil,
				}
			case err := <-wsErrCh:
				errCh <- err
			case <-ctx.Done():

				return
			}
		}
	}()
	return eventsCh, nil
}

func (rt *restTransport) SendEventStore(ctx context.Context, eventStore *EventStore) (*EventStoreResult, error) {
	resp := &restResponse{}
	eventPb := &pb.Event{
		EventID:  eventStore.Id,
		ClientID: eventStore.ClientId,
		Channel:  eventStore.Channel,
		Metadata: eventStore.Metadata,
		Body:     eventStore.Body,
		Store:    true,
		Tags:     eventStore.Tags,
	}
	uri := fmt.Sprintf("%s/send/event", rt.restAddress)
	_, err := rt.newRequest().SetBody(eventPb).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	er := &EventStoreResult{}
	if err := resp.unmarshal(er); err != nil {
		return nil, err
	}
	return er, nil
}

func (rt *restTransport) StreamEventsStore(ctx context.Context, eventsCh chan *EventStore, eventsResultCh chan *EventStoreResult, errCh chan error) {
	uri := fmt.Sprintf("%s/send/stream", rt.wsAddress)
	readCh := make(chan string)
	writeCh := make(chan []byte)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	conn, err := newBiDirectionalWebsocketConn(newCtx, uri, readCh, writeCh, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		errCh <- err
		return
	}
	rt.wsConn = conn
	<-ready

	go func() {
		for {
			select {
			case pbMsg := <-readCh:
				result := &EventStoreResult{}
				err := json.Unmarshal([]byte(pbMsg), result)
				if err != nil {
					errCh <- err
					return
				}
			case err := <-wsErrCh:
				errCh <- err
				return
			case <-newCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case event := <-eventsCh:
			data, _ := json.Marshal(event)
			writeCh <- data
		case err := <-wsErrCh:
			errCh <- err
			return
		case <-newCtx.Done():
			return
		}
	}
}

func (rt *restTransport) SubscribeToEventsStore(ctx context.Context, request *EventsStoreSubscription, errCh chan error) (<-chan *EventStoreReceive, error) {
	eventsCh := make(chan *EventStoreReceive, rt.opts.receiveBufferSize)
	subOption := subscriptionOption{}
	request.SubscriptionType.apply(&subOption)
	uri := fmt.Sprintf("%s/subscribe/events?&client_id=%s&channel=%s&group=%s&subscribe_type=%s&events_store_type_data=%d&events_store_type_value=%d", rt.wsAddress, request.ClientId, request.Channel, request.Group, "events_store", subOption.kind, subOption.value)
	rxChan := make(chan string)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	conn, err := newWebsocketConn(ctx, uri, rxChan, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		return nil, err
	}
	rt.wsConn = conn
	<-ready
	go func() {
		for {
			select {
			case pbMsg := <-rxChan:
				eventReceive := &pb.EventReceive{}
				err := json.Unmarshal([]byte(pbMsg), eventReceive)
				if err != nil {
					errCh <- err
					return
				}
				eventsCh <- &EventStoreReceive{
					Id:       eventReceive.EventID,
					Channel:  eventReceive.Channel,
					Metadata: eventReceive.Metadata,
					Body:     eventReceive.Body,
					ClientId: "",
					Tags:     eventReceive.Tags,
				}

			case err := <-wsErrCh:
				errCh <- err
			case <-ctx.Done():

				return
			}

		}
	}()
	return eventsCh, nil
}

func (rt *restTransport) SendCommand(ctx context.Context, command *Command) (*CommandResponse, error) {
	resp := &restResponse{}
	request := &pb.Request{
		RequestID:            command.Id,
		RequestTypeData:      1,
		ClientID:             command.ClientId,
		Channel:              command.Channel,
		Metadata:             command.Metadata,
		Body:                 command.Body,
		ReplyChannel:         "",
		Timeout:              int32(command.Timeout.Seconds() * 1e3),
		CacheKey:             "",
		CacheTTL:             0,
		Span:                 nil,
		Tags:                 command.Tags,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_sizecache:        0,
	}
	uri := fmt.Sprintf("%s/send/request", rt.restAddress)
	_, err := rt.newRequest().SetBody(request).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	cr := &CommandResponse{}
	if err := resp.unmarshal(cr); err != nil {
		return nil, err
	}
	return cr, nil
}

func (rt *restTransport) SubscribeToCommands(ctx context.Context, request *CommandsSubscription, errCh chan error) (<-chan *CommandReceive, error) {
	commandCh := make(chan *CommandReceive, rt.opts.receiveBufferSize)
	uri := fmt.Sprintf("%s/subscribe/requests?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", rt.wsAddress, request.ClientId, request.Channel, request.Group, "commands")
	rxChan := make(chan string)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	conn, err := newWebsocketConn(ctx, uri, rxChan, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		return nil, err
	}
	rt.wsConn = conn
	<-ready
	go func() {
		for {
			select {
			case pbMsg := <-rxChan:
				request := &pb.Request{}
				err := json.Unmarshal([]byte(pbMsg), request)
				if err != nil {
					errCh <- err
					return
				}

				commandCh <- &CommandReceive{
					Id:         request.RequestID,
					ClientId:   request.ClientID,
					Channel:    request.Channel,
					Metadata:   request.Metadata,
					Body:       request.Body,
					ResponseTo: request.ReplyChannel,
					Tags:       request.Tags,
				}
			case err := <-wsErrCh:
				errCh <- err
			case <-ctx.Done():
				return
			}

		}
	}()
	return commandCh, nil
}

func (rt *restTransport) SendQuery(ctx context.Context, query *Query) (*QueryResponse, error) {
	resp := &restResponse{}
	request := &pb.Request{
		RequestID:       query.Id,
		RequestTypeData: 2,
		ClientID:        query.ClientId,
		Channel:         query.Channel,
		Metadata:        query.Metadata,
		Body:            query.Body,
		ReplyChannel:    "",
		Timeout:         int32(query.Timeout.Seconds() * 1e3),
		CacheKey:        "",
		CacheTTL:        0,
		Span:            nil,
		Tags:            query.Tags,
	}
	uri := fmt.Sprintf("%s/send/request", rt.restAddress)
	_, err := rt.newRequest().SetBody(request).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	qr := &QueryResponse{}
	if err := resp.unmarshal(qr); err != nil {
		return nil, err
	}
	return qr, nil
}

func (rt *restTransport) SubscribeToQueries(ctx context.Context, request *QueriesSubscription, errCh chan error) (<-chan *QueryReceive, error) {
	queryCh := make(chan *QueryReceive, rt.opts.receiveBufferSize)
	uri := fmt.Sprintf("%s/subscribe/requests?&client_id=%s&channel=%s&group=%s&subscribe_type=%s", rt.wsAddress, request.ClientId, request.Channel, request.Group, "queries")
	rxChan := make(chan string)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	conn, err := newWebsocketConn(ctx, uri, rxChan, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		return nil, err
	}
	rt.wsConn = conn
	<-ready
	go func() {
		for {
			select {
			case pbMsg := <-rxChan:
				request := &pb.Request{}
				err := json.Unmarshal([]byte(pbMsg), request)
				if err != nil {
					errCh <- err
					return
				}
				queryCh <- &QueryReceive{
					Id:         request.RequestID,
					ClientId:   request.ClientID,
					Channel:    request.Channel,
					Metadata:   request.Metadata,
					Body:       request.Body,
					ResponseTo: request.ReplyChannel,
					Tags:       request.Tags,
				}
			case err := <-wsErrCh:
				errCh <- err
			case <-ctx.Done():
				return
			}

		}
	}()
	return queryCh, nil
}

func (rt *restTransport) SendResponse(ctx context.Context, response *Response) error {
	resp := &restResponse{}
	request := &pb.Response{
		ClientID:     response.ClientId,
		RequestID:    response.RequestId,
		ReplyChannel: response.ResponseTo,
		Metadata:     response.Metadata,
		Body:         response.Body,
		CacheHit:     false,
		Timestamp:    0,
		Executed:     false,
		Error:        "",
		Span:         nil,
		Tags:         response.Tags,
	}
	uri := fmt.Sprintf("%s/send/response", rt.restAddress)
	_, err := rt.newRequest().SetBody(request).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return err
	}
	if err := resp.unmarshal(nil); err != nil {
		return err
	}
	return nil
}

func (rt *restTransport) SendQueueMessage(ctx context.Context, msg *QueueMessage) (*SendQueueMessageResult, error) {
	resp := &restResponse{}
	uri := fmt.Sprintf("%s/queue/send", rt.restAddress)
	_, err := rt.newRequest().SetBody(msg.QueueMessage).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	result := &SendQueueMessageResult{}
	if err := resp.unmarshal(result); err != nil {
		return nil, err
	}
	return result, nil
}

func (rt *restTransport) SendQueueMessages(ctx context.Context, msgs []*QueueMessage) ([]*SendQueueMessageResult, error) {
	resp := &restResponse{}
	br := &pb.QueueMessagesBatchRequest{
		BatchID:  nuid.New().Next(),
		Messages: []*pb.QueueMessage{},
	}
	for _, msg := range msgs {
		br.Messages = append(br.Messages, msg.QueueMessage)
	}
	uri := fmt.Sprintf("%s/queue/send_batch", rt.restAddress)
	_, err := rt.newRequest().SetBody(br).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}

	batchResults := &pb.QueueMessagesBatchResponse{}
	if err := resp.unmarshal(batchResults); err != nil {
		return nil, err
	}
	var results []*SendQueueMessageResult
	for _, result := range batchResults.Results {
		results = append(results, &SendQueueMessageResult{
			MessageID:    result.MessageID,
			SentAt:       result.SentAt,
			ExpirationAt: result.ExpirationAt,
			DelayedTo:    result.DelayedTo,
			IsError:      result.IsError,
			Error:        result.Error,
		})
	}
	return results, nil
}

func (rt *restTransport) ReceiveQueueMessages(ctx context.Context, req *ReceiveQueueMessagesRequest) (*ReceiveQueueMessagesResponse, error) {
	resp := &restResponse{}
	request := &pb.ReceiveQueueMessagesRequest{
		RequestID:           req.RequestID,
		ClientID:            req.ClientID,
		Channel:             req.Channel,
		MaxNumberOfMessages: req.MaxNumberOfMessages,
		WaitTimeSeconds:     req.WaitTimeSeconds,
		IsPeak:              req.IsPeak,
	}
	uri := fmt.Sprintf("%s/queue/receive", rt.restAddress)
	_, err := rt.newRequest().SetBody(request).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	res := &ReceiveQueueMessagesResponse{}
	if err := resp.unmarshal(res); err != nil {
		return nil, err
	}
	return res, nil
}

func (rt *restTransport) AckAllQueueMessages(ctx context.Context, req *AckAllQueueMessagesRequest) (*AckAllQueueMessagesResponse, error) {
	resp := &restResponse{}
	request := &pb.AckAllQueueMessagesRequest{
		RequestID:       req.RequestID,
		ClientID:        req.ClientID,
		Channel:         req.Channel,
		WaitTimeSeconds: req.WaitTimeSeconds,
	}
	uri := fmt.Sprintf("%s/queue/ack_all", rt.restAddress)
	_, err := rt.newRequest().SetBody(request).SetResult(resp).SetError(resp).Post(uri)
	if err != nil {
		return nil, err
	}
	res := &AckAllQueueMessagesResponse{}
	if err := resp.unmarshal(res); err != nil {
		return nil, err
	}
	return res, nil
}

func (rt *restTransport) StreamQueueMessage(ctx context.Context, reqCh chan *pb.StreamQueueMessagesRequest, resCh chan *pb.StreamQueueMessagesResponse, errCh chan error, doneCh chan bool) {
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	uri := fmt.Sprintf("%s/queue/stream", rt.wsAddress)
	readCh := make(chan string)
	writeCh := make(chan []byte)
	ready := make(chan struct{}, 1)
	wsErrCh := make(chan error, 1)
	conn, err := newBiDirectionalWebsocketConn(newCtx, uri, readCh, writeCh, ready, wsErrCh, rt.opts.authToken)
	if err != nil {
		errCh <- err
		return
	}
	rt.wsConn = conn
	<-ready

	defer func() {
		doneCh <- true
	}()
	go func() {
		for {
			select {
			case pbMsg := <-readCh:
				res := &pb.StreamQueueMessagesResponse{}
				err := json.Unmarshal([]byte(pbMsg), res)
				if err != nil {
					errCh <- err
					return
				}
				select {
				case resCh <- res:
				case <-newCtx.Done():
					return
				}
			case err := <-wsErrCh:
				errCh <- err
				return
			case <-newCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case req := <-reqCh:
			data, _ := json.Marshal(req)
			writeCh <- data
		case err := <-wsErrCh:
			errCh <- err
			return
		case <-newCtx.Done():
			return
		}
	}

}

func (rt *restTransport) QueuesInfo(ctx context.Context, filter string) (*QueuesInfo, error) {
	resp := &restResponse{}
	uri := fmt.Sprintf("%s/queue/info", rt.restAddress)
	req := &pb.QueuesInfoRequest{
		RequestID: uuid.New(),
		QueueName: filter,
	}
	_, err := rt.newRequest().SetBody(req).SetResult(resp).SetError(resp).Get(uri)
	if err != nil {
		return nil, err
	}
	result := &pb.QueuesInfoResponse{}
	if err := resp.unmarshal(result); err != nil {
		return nil, err
	}
	return fromQueuesInfoPb(result.Info), nil
}
func (rt *restTransport) Close() error {
	if rt.wsConn != nil {
		return rt.wsConn.Close()
	}
	return nil
}
func (rt *restTransport) GetGRPCRawClient() (pb.KubemqClient, error) {
	return nil, fmt.Errorf("this function is not supported in rest transport")
}
