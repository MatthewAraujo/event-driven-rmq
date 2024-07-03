package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// The connection ued by the client
	conn *amqp.Connection
	// Channel  is used to process /  Send messages
	ch *amqp.Channel
}

func ConnectRabbit(username, password, host, vhost, caCert, clientCert, clientKey string) (*amqp.Connection, error) {
	ca, err := os.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, err
	}

	rootCas := x509.NewCertPool()
	rootCas.AppendCertsFromPEM(ca)

	tlsConfig := &tls.Config{
		RootCAs: rootCas,
		Certificates: []tls.Certificate{
			cert,
		},
	}

	// Connect to RabbitMQ
	conn, err := amqp.DialTLS(fmt.Sprintf("amqps://%s:%s@%s/%s", username, password, host, vhost), tlsConfig)
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

	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}

	return RabbitClient{conn: conn, ch: ch}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, nil
}

// CreateBinding will bind the current channel to given exchange using the routingkey provided
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// send is used to publish pauload onto an exchange with a routing key
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {

	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(
		ctx,
		exchange,
		routingKey,
		true,
		false,
		options)

	if err != nil {
		return err
	}

	log.Printf("Sent message with delivery tag: %d", confirmation.DeliveryTag)
	return nil
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

// ApplyQos-
// prefetch count - an integer on how many unackowledge messeges the serve can send
// prefetch - is int of how many bytes
// global - Determines if the rule should be applied to all consumers
func (rc RabbitClient) SetQos(prefetchCount, prefetchSize int, global bool) error {

	return rc.ch.Qos(prefetchCount, prefetchSize, global)
}
