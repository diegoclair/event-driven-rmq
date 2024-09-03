package main

import (
	"context"
	"log"
	"time"

	"github.com/diegoclair/event-driven-rmq/internal"
	"golang.org/x/sync/errgroup"
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// topic example, messages will be balanced between consumers, if you have 10 messages and 2 consumers, each consumer will receive 5 messages
	// fanout example, messages will be sent to all consumers, if you have 10 messages and 2 consumers, each consumer will receive 10 messages

	//topicExchangeExample(ctx, client)
	fanoutExchangeExample(ctx, client)
}

func fanoutExchangeExample(ctx context.Context, client *internal.RabbitMQClient) {
	// empty name will create a random queue (usually when you have publishers and subscribers, you want to create a random queue)
	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	// create a fanout exchange
	err = client.CreateExchange("f_costumers_events", "fanout", true, false)
	if err != nil {
		panic(err)
	}

	// as it is a fanout exchange, the routing key doesn't matter
	err = client.BindQueueToExchange(queue.Name, "f_costumers_events", "")
	if err != nil {
		panic(err)
	}

	startConsume(ctx, client, queue.Name)
}

func topicExchangeExample(ctx context.Context, client *internal.RabbitMQClient) {
	startConsume(ctx, client, "costumers")
}

func startConsume(ctx context.Context, client *internal.RabbitMQClient, queueName string) {
	var blockForever chan struct{}

	consumerName := "email-service"
	messageBus, err := client.Consume(ctx, queueName, consumerName, false)
	if err != nil {
		panic(err)
	}

	// 15 seconds for each task on the goroutine/consumer
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// errogroup allows to manage multiple goroutines
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(10)

	go func() {
		for message := range messageBus {
			msg := message // Create a new variable to avoid closure issues
			// spawn a new goroutine for each message
			g.Go(func() error {
				log.Printf("Received message: %v\n", msg)
				time.Sleep(10 * time.Second)
				err := msg.Ack(false)
				if err != nil {
					log.Printf("Error acknowledging msg: %v", err)
				}

				log.Printf("Acknowledged msg %s", msg.MessageId)
				return nil
			})
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-blockForever

	log.Printf(" [*] Exiting consumer")

	if err := g.Wait(); err != nil {
		log.Printf("Error: %v", err)
	}
}
