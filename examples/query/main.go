package main

import (
	"context"
	"github.com/kubemq-io/kubemq-go"
	"log"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := kubemq.NewClient(ctx,
		kubemq.WithAddress("localhost", 50000),
		kubemq.WithClientId("test-query-client-id"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	channel := "testing_query_channel"

	go func() {
		errCh := make(chan error)
		queriesCh, err := client.SubscribeToQueries(ctx, channel, "", errCh)
		if err != nil {
			log.Fatal(err)
		}
		for {
			select {
			case err := <-errCh:
				log.Fatal(err)
				return
			case query := <-queriesCh:
				log.Printf("Query Recevied:\nId %s\nChannel: %s\nMetadata: %s\nBody: %s\n", query.Id, query.Channel, query.Metadata, query.Body)
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
			case <-ctx.Done():
				return
			}
		}

	}()
	// give some time to connect a receiver
	time.Sleep(time.Second)
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
	log.Printf("Response Recevied:\nQueryID: %s\nExecutedAt:%s\nMetadat: %s\nBody: %s\n", response.QueryId, response.ExecutedAt, response.Metadata, response.Body)

}
