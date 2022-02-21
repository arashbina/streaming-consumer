package stream

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

type Client struct {
	config   Config
	consumer *kafka.Consumer
	producer *kafka.Producer
}

type Config struct {
	SchemaTopic        string
	OutboxTopicPattern string
	KafkaUser          string
	KafkaSecret        string
}

func NewClient(cfg Config) (*Client, error) {

	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092",
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     cfg.KafkaUser,
		"sasl.password":     cfg.KafkaSecret,
		"group.id":          "dataStreamConsumer",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		return nil, err
	}

	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092",
		"sasl.mechanisms":   "PLAIN",
		"security.protocol": "SASL_SSL",
		"sasl.username":     cfg.KafkaUser,
		"sasl.password":     cfg.KafkaSecret,
	})

	if err != nil {
		return nil, err
	}

	c := Client{
		config:   cfg,
		consumer: kc,
		producer: kp,
	}

	log.Println("connected to kafka")
	return &c, nil
}

func (kc *Client) Consume() {

	err := kc.consumer.SubscribeTopics([]string{kc.config.OutboxTopicPattern, kc.config.SchemaTopic}, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := kc.consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}

func (kc *Client) Shutdown() error {
	kc.producer.Close()
	return kc.consumer.Close()
}
