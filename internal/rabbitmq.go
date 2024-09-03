package internal

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	// The connection used by the client
	conn *amqp.Connection
	// The channel is used to process / send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vHost string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vHost))
}

func NewRabbitMQClient(conn *amqp.Connection) (*RabbitMQClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// this enables the possibility to use PublishWithDeferredConfirmWithContext function
	// if err := ch.Confirm(false); err != nil {
	// 	return nil, err
	// }

	return &RabbitMQClient{conn: conn, ch: ch}, nil
}

func (r *RabbitMQClient) Close() {
	r.ch.Close()
	r.conn.Close()
}

func (r *RabbitMQClient) CreateQueue(name string, durable, autoDelete bool) (amqp.Queue, error) {
	q, err := r.ch.QueueDeclare(name, durable, autoDelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, err
}

func (r *RabbitMQClient) CreateExchange(name, kind string, durable, autoDelete bool) error {
	return r.ch.ExchangeDeclare(name, kind, durable, autoDelete, false, false, nil)
}

// BindQueue binds a queue (current channel) to an exchange with a routing key provided
//
// rmq: QueueBind binds an exchange to a queue so that publishings to the exchange will be routed to the queue when the publishing routing key matches the binding routing key.
func (r *RabbitMQClient) BindQueueToExchange(queueName, exchangeName, routingKey string) error {
	// leaving nowait as false, having it as false will make the channel return an error if it fails to bind
	return r.ch.QueueBind(queueName, routingKey, exchangeName, false, nil)
}

// Publish sends a message to the exchange with the routing key provided
func (r *RabbitMQClient) Publish(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	return r.ch.PublishWithContext(ctx,
		exchange,
		routingKey,
		// NOTE mandatory is used to determine if an error should be returned upon failure to deliver the message to at least one queue
		true,
		// immediate Removed in RabbitMQ 3.0.0 and later https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0#removal-of-immediate-flag
		false,
		options,
	)
}

// PublishDeferredConfirm sends a message to the exchange with the routing key provided and waits for the confirmation (ack) from the broker
func (r *RabbitMQClient) PublishDeferredConfirm(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	confirmation, err := r.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchange,
		routingKey,
		// NOTE mandatory is used to determine if an error should be returned upon failure to deliver the message to at least one queue
		true,
		// immediate Removed in RabbitMQ 3.0.0 and later https://blog.rabbitmq.com/posts/2012/11/breaking-things-with-rabbitmq-3-0#removal-of-immediate-flag
		false,
		options,
	)
	if err != nil {
		return err
	}

	// Wait for the confirmation from the broker and not from ack from the consumer
	// This is useful when you want to make sure the message was delivered to the broker'
	// it means that the message was delivered to the exchange, but not if it was processed by the consumer
	confirmed := confirmation.Wait()
	if !confirmed {
		return fmt.Errorf("message was not properly confirmed")
	}

	log.Printf("Message %s was confirmed", options.MessageId)

	return nil
}

func (r *RabbitMQClient) Consume(ctx context.Context, queueName, consumerName string, autoAck bool) (<-chan amqp.Delivery, error) {
	return r.ch.ConsumeWithContext(ctx, queueName, consumerName, autoAck, false, false, false, nil)
}
