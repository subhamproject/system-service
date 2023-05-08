package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func getKafkaReader(kafkaURL, topic, groupID string) *kafka.Reader {
	clientCertFile := GetEnvParam("KAFKA_CLIENT_CERT", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/kafka.system.cert")
	clientKeyFile := GetEnvParam("KAFKA_CLIENT_KEY", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/kafka.system.key")
	// caCertFile := GetEnvParam("KAFKA_CA_CERT", "/home/om/go/src/github.com/subhamproject/devops-demo/certs/kafka.user.pem")

	cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		log.Fatal(err)
	}
	if err != nil {
		log.Fatal(err)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
		TLS:       cfg,
	}
	brokers := strings.Split(kafkaURL, ",")
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topic:   topic,
		Dialer:  dialer,
	})
}

func main() {
	// get kafka reader using environment variables.
	groupID := GetEnvParam("KAFKA_GROUP_ID", "demo-kafka")
	topic := GetEnvParam("KAFKA_TOPIC", "demoTopic")
	kafkaURL := GetEnvParam("KAFKA_SERVERS", "localhost:19091,localhost:29092,localhost:39093")
	fmt.Println("kafka kafkaURL: ", kafkaURL)

	kafkaReader := getKafkaReader(kafkaURL, topic, groupID)

	defer kafkaReader.Close()

	fmt.Println("start consuming ... !!")
	for {
		m, err := kafkaReader.ReadMessage(context.Background())
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Printf("received message at topic:%v partition:%v offset:%v	%s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
}

// GetEnvParam : return string environmental param if exists, otherwise return default
func GetEnvParam(param string, dflt string) string {
	if v, exists := os.LookupEnv(param); exists {
		return v
	}
	return dflt
}
