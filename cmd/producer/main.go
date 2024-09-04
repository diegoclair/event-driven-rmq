package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/diegoclair/event-driven-rmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

/*
TODO - next steps

add tls configuration for the rabbitmq server
add configuration file for users and permissions
*/
func main() {
	client := getClient()
	defer client.Close()

	consumerClient := getClient()
	defer consumerClient.Close()

	// topic example, messages will be balanced between consumers, if you have 10 messages and 2 consumers, each consumer will receive 5 messages
	// fanout example, messages will be sent to all consumers, if you have 10 messages and 2 consumers, each consumer will receive 10 messages

	//topicExchangeExample(client)
	//fanoutExchangeExample(client)
	directExchangeExample(context.Background(), client, consumerClient)
}

func getClient() *internal.RabbitMQClient {
	conn, err := internal.ConnectRabbitMQ("diego", "secret", "localhost:5672", "eventdriven")
	if err != nil {
		panic(err)
	}

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}

	return client
}

func directExchangeExample(ctx context.Context, client *internal.RabbitMQClient, consumerClient *internal.RabbitMQClient) {
	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	err = consumerClient.CreateExchange("customer_callbacks", "direct", true, false)
	if err != nil {
		panic(err)
	}

	err = consumerClient.CreateExchange("d_customers_events", "fanout", true, false)
	if err != nil {
		panic(err)
	}

	err = consumerClient.BindQueueToExchange(queue.Name, "customer_callbacks", queue.Name)
	if err != nil {
		panic(err)
	}

	messageBus, err := consumerClient.Consume(ctx, queue.Name, "customer-api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for msg := range messageBus {
			log.Printf("Message received callback %s\n", msg.CorrelationId)
		}
	}()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	exchangeName := "d_customers_events"
	for i := 0; i < 10; i++ {
		err = client.Publish(ctx, exchangeName, "costumers.created.us", amqp091.Publishing{
			ContentType:   "text/plain",
			Body:          []byte(fmt.Sprintf("Hello World %d", i)),
			DeliveryMode:  amqp091.Persistent,
			ReplyTo:       queue.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
		})
		if err != nil {
			panic(err)
		}
	}

	log.Println("Producer started", client)
	var block chan bool
	<-block
}

func fanoutExchangeExample(client *internal.RabbitMQClient) {
	exchangeName := "f_customers_events"
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		// fanout we don't need to provide a routing key
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
	exchangeName := "customers_events"

	_, err := client.CreateQueue("costumers", true, false)
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		err = client.Publish(ctx, exchangeName, "costumers.created.us", amqp091.Publishing{
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
