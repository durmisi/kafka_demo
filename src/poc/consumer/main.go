package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/segmentio/kafka-go"
)

type MessageData struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		GroupID:  "consumer-group-1",
		Topic:    "my-topic",
		MaxBytes: 10e6, // 10MB
	})

	var wg sync.WaitGroup
	concurrency := 15

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumeMessages(r)
		}()
	}

	wg.Wait()

	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

func consumeMessages(r *kafka.Reader) {
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v\n", err)
			continue
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		fmt.Printf("-----------------------------------------------------------------------------")
	}
}
