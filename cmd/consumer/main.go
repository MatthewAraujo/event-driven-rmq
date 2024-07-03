package main

import (
	"context"
	"log"
	"time"

	"github.com/MatthewAraujo/event-driven-rmq/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbit("matthew", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	publishConn, err := internal.ConnectRabbit("matthew", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer publishConn.Close()

	publishClient, err := internal.NewRabbitClient(publishConn)
	if err != nil {
		panic(err)
	}
	defer publishClient.Close()

	rabbitClient, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer rabbitClient.Close()

	queue, err := rabbitClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := rabbitClient.CreateBinding(queue.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messageBus, err := rabbitClient.Consume(queue.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	var blocking chan struct{}

	// set a timeout for 15secs
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
				if err := publishClient.Send(ctx, "customer_callbacks", msg.ReplyTo, amqp.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp.Persistent,
					ReplyTo:       msg.ReplyTo,
					CorrelationId: msg.CorrelationId,
					Body:          []byte(`RPC Complete`),
				}); err != nil {
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
