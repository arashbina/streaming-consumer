package main

import (
	"github.com/arashbina/streaming-consumer/internal/logic"
	"github.com/arashbina/streaming-consumer/internal/storage/s3"
	"github.com/arashbina/streaming-consumer/internal/stream"
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	DataStreamSchema   = "data_stream_schema"
	OutboxTopicPattern = "^outbox_[a-z]*_.*"
)

func main() {

	userKafka := os.Getenv("KAFKA_USER")
	passKafka := os.Getenv("KAFKA_SECRET")

	cfg := stream.Config{
		SchemaTopic:        DataStreamSchema,
		OutboxTopicPattern: OutboxTopicPattern,
		KafkaUser:          userKafka,
		KafkaSecret:        passKafka,
	}

	kc, err := stream.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	store, err := s3.NewStore()
	if err != nil {
		panic(err)
	}

	l := logic.New(store, kc)
	if l == nil {
		panic("could not create the logic layer")
	}

	go l.Consume()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case sig := <-shutdown:
		if sig == syscall.SIGSTOP {
			log.Fatalln("sig stop shutdown")
		}
		if err = kc.Shutdown(); err != nil {
			log.Fatalf("error shutting down stream: %s", err)
		}
	}
}
