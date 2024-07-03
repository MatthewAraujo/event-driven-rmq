package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/MatthewAraujo/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbit("matthew", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	consumeConn, err := internal.ConnectRabbit("matthew", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	messageBus, err := consumeClient.Consume(queue.Name, "constumer-api", false)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("New message: %s", message.CorrelationId)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp091.Persistent,
			ReplyTo:       queue.Name,
			Body:          []byte(`an cool message between customers`),
			CorrelationId: fmt.Sprintf("correlation-%d", i),
		}); err != nil {
			panic(err)
		}
	}

	log.Println(client)
	var blocking chan struct{}
	<-blocking
}
