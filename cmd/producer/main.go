package main

import (
	"log"
	"time"

	"github.com/MatthewAraujo/event-driven-rmq/internal"
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

	time.Sleep(time.Second * 5)

	log.Println(rabbitClient)
}
