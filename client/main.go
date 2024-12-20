package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	consumer, err := CreateKafkaConsumer([]string{"localhost:9092"})
	if err != nil {
		panic(err)
	}

	topic := "temperature"

	worker, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Worker started")

	// os signal of SIGINT to channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	doneChan := make(chan struct{})

	msgCnt := 0

	go func() {
		for {
			select {
			case err := <-worker.Errors():
				fmt.Println(err)
			case msg := <-worker.Messages():
				msgCnt++
				fmt.Printf("Receiver temperature, count %d: | topic (%s), message (%s) \n", msgCnt, string(msg.Topic), string(msg.Value))
			case <-sigChan:
				fmt.Println("interrupt is detected")
				doneChan <- struct{}{}
			}
		}
	}()

	<-doneChan
	fmt.Println("processed", msgCnt, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func CreateKafkaConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	return sarama.NewConsumer(brokers, config)
}
