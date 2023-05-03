package main

import (
	"fmt"

	"os"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// GetEnvParam : return string environmental param if exists, otherwise return default
func GetEnvParam(param string, dflt string) string {
	if v, exists := os.LookupEnv(param); exists {
		return v
	}
	return dflt
}

func main() {
	fmt.Println("system service is ready to read from Kafka...")
	host := GetEnvParam("KAFKA_HOST", "localhost")
	port := GetEnvParam("KAFKA_PORT", "9092")
	topic := GetEnvParam("KAFKA_TOPIC", "demoTopic")
	maxPollInterval := GetEnvParam("KAFKA_MAX_POLL_INTERVAL_MS", "60000")

	fmt.Printf("Kafka host:%s , ,port:%s, ,topic:%s \n", host, port, topic)
	kafkaServer := host + ":" + port

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":    kafkaServer,
		"group.id":             "myGroup",
		"auto.offset.reset":    "earliest",
		"max.poll.interval.ms": maxPollInterval,
	})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics([]string{topic}, nil)

	// A signal handler or similar could be used to set this to false to break the loop.
	run := true

	for run {
		ev := c.Poll(1000)
		switch e := ev.(type) {
		case *kafka.Message:
			// application-specific processing
			if e.TopicPartition.Error != nil {
				fmt.Printf("Failed to deliver message: %v\n", e.TopicPartition.Error)
			} else {
				fmt.Printf("Received record from topic %s partition [%d] : %v\n", *e.TopicPartition.Topic, e.TopicPartition.Partition, string(e.Value))
			}
		case kafka.Error:
			fmt.Printf("Consumer error: %v (%v)\n", err, ev)
			run = false
		default:
			//fmt.Printf("Ignored %v\n", e)
		}
	}

	c.Close()
}
