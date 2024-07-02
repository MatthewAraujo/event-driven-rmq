package main

import (
	"context"
	"log"
	"time"

	"github.com/MatthewAraujo/event-driven-rmq/internal"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbit("matthew", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	rabbitClient, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer rabbitClient.Close()

	messageBus, err := rabbitClient.Consume("customers_created", "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}

	// set a timeout for 15secs
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// errgroup allows us concurrent tasks-
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			//Spawn a worker
			msg := message
			g.Go(func() error {
				log.Printf("New message: %s", msg.Body)
				time.Sleep(time.Second * 10)
				if err := msg.Ack(false); err != nil {
					log.Printf("Error Acking message: %s", err)
					return err
				}
				log.Printf("Message acknowledged %s", msg.MessageId)
				return nil
			})
		}
	}()

	log.Printf("Waiting for messages")

	<-blocking

}
