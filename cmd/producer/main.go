package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/diegoclair/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("diego", "secret", "localhost:5672", "eventdriven")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// topic example, messages will be balanced between consumers, if you have 10 messages and 2 consumers, each consumer will receive 5 messages
	// fanout example, messages will be sent to all consumers, if you have 10 messages and 2 consumers, each consumer will receive 10 messages

	//topicExchangeExample(client)
	fanoutExchangeExample(client)
}

func fanoutExchangeExample(client *internal.RabbitMQClient) {
	exchangeName := "f_costumers_events"
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		// Publish a message
		err = client.Publish(ctx, exchangeName, "", amqp091.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(fmt.Sprintf("Hello World %d", i)),
			DeliveryMode: amqp091.Persistent,
		})
		if err != nil {
			panic(err)
		}

	}

	log.Println("Producer started", client)
}

// for exchange example, the producer will create the exchange and the queues, because it will
func topicExchangeExample(client *internal.RabbitMQClient) {
	exchangeName := "costumers_events"

	_, err := client.CreateQueue("costumers", true, false)
	if err != nil {
		panic(err)
	}

	_, err = client.CreateQueue("customers_test", false, true)
	if err != nil {
		panic(err)
	}

	err = client.CreateExchange(exchangeName, "topic", true, false)
	if err != nil {
		panic(err)
	}

	err = client.BindQueueToExchange("costumers", exchangeName, "costumers.created.*")
	if err != nil {
		panic(err)
	}

	err = client.BindQueueToExchange("customers_test", exchangeName, "costumers.*")
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		// Publish a message
		err = client.Publish(ctx, exchangeName, "costumers.created.us", amqp091.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(fmt.Sprintf("Hello World %d", i)),
			DeliveryMode: amqp091.Persistent,
		})
		if err != nil {
			panic(err)
		}

		// Publish a transient message
		err = client.PublishDeferredConfirm(ctx, exchangeName, "costumers.test", amqp091.Publishing{
			ContentType:  "text/plain",
			Body:         []byte("Uncool and unDurable Hello World"),
			DeliveryMode: amqp091.Transient,
		})
		if err != nil {
			panic(err)
		}
	}

	log.Println("Producer started", client)
}
