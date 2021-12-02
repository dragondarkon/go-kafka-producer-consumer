package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Massege struct {
	MsgId  string `json:"msg_id"`
	Sender string `json:"sender"`
	Msg    string `json:"msg"`
}

func main() {

	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://mongo_db:27017"))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	topic := "test"
	worker, err := connectConsumer([]string{"localhost:9092", "kafka:9093"})
	if err != nil {
		panic(err)
	}

	// Calling ConsumePartition. It will open one connection per broker
	// and share it for all partitions that live on it.
	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	fmt.Println("Consumer started ")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	// Count how many message processed
	msgCount := 0

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				res := map[string]interface{}{}
				if err := json.Unmarshal(msg.Value, &res); err != nil {
					fmt.Println(err)
				}
				jsonString, _ := json.Marshal(res)
				m := Massege{}
				json.Unmarshal(jsonString, &m)
				t := time.Now()
				fmt.Printf("#3 %s++ %s %s \n", string(m.Sender), string(m.Msg), t.Format("2006-01-02 15:04:05"))
				// begin insertOne
				coll := client.Database("test").Collection("messages")
				_, err := coll.InsertOne(context.TODO(), m)
				if err != nil {
					panic(err)
				}
				// end insertOne
			case <-sigchan:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	fmt.Println("Processed", msgCount, "messages")

	if err := worker.Close(); err != nil {
		panic(err)
	}

}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
