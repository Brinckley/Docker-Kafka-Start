package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"gopkg.in/Shopify/sarama.v1"
)

var kafkaBrokers = os.Getenv("KAFKA_PEERS")       // all the brokers we need to talk to
var kafkaTopic = os.Getenv("KAFKA_TOPIC")         // topic we need to produce messages on
var kafkaVersion = os.Getenv("KAFKA_VERSION")     // version of kafka
var kafkaGroup = os.Getenv("KAFKA_GROUP")         // group name

type Consumer struct {
	ready chan bool
}

func main() {
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	config := sarama.NewConfig()                                                                  // creating new config, passing the version
	config.Version = version
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin                   // rebalancing our messages
	config.Consumer.Offsets.Initial = sarama.OffsetOldest                                         // initial offset

	ctx, cancel := context.WithCancel(context.Background())
	client, err := sarama.NewConsumerGroup(strings.Split(kafkaBrokers, ","), kafkaGroup, config)  // specifying new consumer group, passing config to talk to kafka 
	
	if err != nil {
		fmt.Printf("Failed to init Kafka consumer group: %s\n", err)
		panic(err)
	}


	consumer := Consumer {
		ready : make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// consuming in infinite loop
			// if server-side get rebalanced, the consumer session will need to be recreated to get new claims
			if err := client.Consume(ctx, strings.Split(kafkaTopic, ","), &consumer); err != nil {
				fmt.Printf("Error from consumer: %v\n", err)
				panic(err)
			}
			// checking if context was cancelled, signaling the consumer to stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready                                                                               // Await till the consumer has been set up
	fmt.Println("Sarama consumer up and running!....")

	// as long as we are using docker, all errors should be treated well
	// all this is written to prevent programm from falling badly
	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		fmt.Println("terminating: context cancelled")
	case <-sigterm:
		fmt.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()

	if err = client.Close(); err != nil {
		fmt.Printf("Error closing client: %v", err)
		panic(err)
	}
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine
	for msg := range claim.Messages() {

		fmt.Printf("Partition:\t%d\n", msg.Partition)
		fmt.Printf("Offset:\t%d\n", msg.Offset)
		fmt.Printf("Key:\t%s\n", string(msg.Key))
		fmt.Printf("Value:\t%s\n", string(msg.Value))
		fmt.Printf("Topic:\t%s\n", msg.Topic)
		fmt.Println()

		session.MarkMessage(msg, "")
	}

	return nil
}
