package main

// import (
// 	"fmt"

// 	"os"

// 	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
// )

// // GetEnvParam : return string environmental param if exists, otherwise return default
// func GetEnvParam(param string, dflt string) string {
// 	if v, exists := os.LookupEnv(param); exists {
// 		return v
// 	}
// 	return dflt
// }

// func main() {
// 	fmt.Println("system service is ready to read from Kafka...")
// 	host := GetEnvParam("KAFKA_HOST", "localhost")
// 	port := GetEnvParam("KAFKA_PORT", "9092")
// 	topic := GetEnvParam("KAFKA_TOPIC", "demoTopic")
// 	maxPollInterval := GetEnvParam("KAFKA_MAX_POLL_INTERVAL_MS", "60000")

// 	fmt.Printf("Kafka host:%s , ,port:%s, ,topic:%s \n", host, port, topic)
// 	kafkaServer := host + ":" + port

// 	c, err := kafka.NewConsumer(&kafka.ConfigMap{
// 		"bootstrap.servers":    kafkaServer,
// 		"group.id":             "myGroup",
// 		"auto.offset.reset":    "earliest",
// 		"max.poll.interval.ms": maxPollInterval,
// 	})

// 	if err != nil {
// 		panic(err)
// 	}

// 	c.SubscribeTopics([]string{topic}, nil)

// 	// A signal handler or similar could be used to set this to false to break the loop.
// 	run := true

// 	for run {
// 		ev := c.Poll(1000)
// 		switch e := ev.(type) {
// 		case *kafka.Message:
// 			// application-specific processing
// 			if e.TopicPartition.Error != nil {
// 				fmt.Printf("Failed to deliver message: %v\n", e.TopicPartition.Error)
// 			} else {
// 				fmt.Printf("Received record from topic %s partition [%d] : %v\n", *e.TopicPartition.Topic, e.TopicPartition.Partition, string(e.Value))
// 			}
// 		case kafka.Error:
// 			fmt.Printf("Consumer error: %v (%v)\n", err, ev)
// 			run = false
// 		default:
// 			//fmt.Printf("Ignored %v\n", e)
// 		}
// 	}

// 	c.Close()
// }

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {

	clientCertFile := GetEnvParam("KAFKA_CLIENT_CERT", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/system.cert.pem")
	clientKeyFile := GetEnvParam("KAFKA_CLIENT_KEY", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/system.key.pem")
	caCertFile := GetEnvParam("KAFKA_CA_CERT", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/kafka-ca.crt")
	kafkaservers := GetEnvParam("KAFKA_SERVERS", "kafka1:19091,kafka2:29092,kafka3:39093")
	servers := strings.Split(kafkaservers, ",")

	fmt.Println("kafka servers: ", servers)
	tlsConfig, err := NewTLSConfig(clientCertFile, clientKeyFile, caCertFile)
	if err != nil {
		log.Fatal(err)
	}
	// This can be used on test server if domain does not match cert:
	// tlsConfig.InsecureSkipVerify = true

	consumerConfig := sarama.NewConfig()
	consumerConfig.Net.TLS.Enable = true
	consumerConfig.Net.TLS.Config = tlsConfig

	client, err := sarama.NewClient(servers, consumerConfig)
	if err != nil {
		log.Fatalf("unable to create kafka client: %q", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	consumerLoop(consumer, "demoTopic")
}

// NewTLSConfig generates a TLS configuration used to authenticate on server with
// certificates.
// Parameters are the three pem files path we need to authenticate: client cert, client key and CA cert.
func NewTLSConfig(clientCertFile, clientKeyFile, caCertFile string) (*tls.Config, error) {
	tlsConfig := tls.Config{}

	// Load client cert
	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return &tlsConfig, err
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	// Load CA cert
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return &tlsConfig, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig.RootCAs = caCertPool

	tlsConfig.BuildNameToCertificate()
	return &tlsConfig, err
}

func consumerLoop(consumer sarama.Consumer, topic string) {
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Println("unable to fetch partition IDs for the topic", topic, err)
		return
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var wg sync.WaitGroup
	for partition := range partitions {
		wg.Add(1)
		go func() {
			consumePartition(consumer, int32(partition), signals)
			wg.Done()
		}()
	}
	wg.Wait()
}

func consumePartition(consumer sarama.Consumer, partition int32, signals chan os.Signal) {
	log.Println("Receving on partition", partition)
	partitionConsumer, err := consumer.ConsumePartition("test", partition, sarama.OffsetNewest)
	if err != nil {
		log.Println(err)
		return
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumed := 0
ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Consumed message offset %d\nData: %s\n", msg.Offset, msg.Value)
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}
	log.Printf("Consumed: %d\n", consumed)
}

// GetEnvParam : return string environmental param if exists, otherwise return default
func GetEnvParam(param string, dflt string) string {
	if v, exists := os.LookupEnv(param); exists {
		return v
	}
	return dflt
}
