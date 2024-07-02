package internal

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// The connection ued by the client
	conn *amqp.Connection
	// Channel  is used to process /  Send messages
	ch *amqp.Channel
}

func ConnectRabbit(username, password, host, vhost string) (*amqp.Connection, error) {
	// Connect to RabbitMQ
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vhost))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewRabbitClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}
	return RabbitClient{conn: conn, ch: ch}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
	_, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	return err
}

// CreateBinding will bind the current channel to given exchange using the routingkey provided
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// send is used to publish pauload onto an exchange with a routing key
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return rc.ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		//mandatory is used to determine if an error should be returned upon failure
		true,
		//immediate is used to determine if the message should be returned to the consumer immediately
		// immediate Removed in MQ 3
		false,
		options,
	)
}

func (rc RabbitClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(
		queue,
		consumer,
		autoAck,
		false,
		false,
		false,
		nil,
	)
}
