package main

import (
	"context"
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

	rabbitClient, err := internal.NewRabbitClient(conn)
	if err != nil {
		panic(err)
	}
	defer rabbitClient.Close()

	if err := rabbitClient.CreateQueue("customers_created", true, false); err != nil {
		panic(err)
	}

	if err := rabbitClient.CreateQueue("customers_test", false, true); err != nil {
		panic(err)
	}

	if err := rabbitClient.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
		panic(err)
	}

	if err := rabbitClient.CreateBinding("customers_test", "customers.*", "customer_events"); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if err := rabbitClient.Send(ctx, "customer_events", "customers.created.us", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Persistent,
		Body:         []byte(`an cool message between customers`),
	}); err != nil {
		panic(err)
	}

	if err := rabbitClient.Send(ctx, "customer_events", "customers.test", amqp091.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp091.Transient,
		Body:         []byte(`an uncool undurable message`),
	}); err != nil {
		panic(err)
	}

	time.Sleep(time.Second * 5)

	log.Println(rabbitClient)
}
