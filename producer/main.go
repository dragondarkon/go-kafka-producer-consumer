package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Req struct {
	MsgId  string `json:"msg_id"`
	Sender string `json:"sender"`
	Msg    string `json:"msg"`
}

// type Msg struct {
// 	Partition int32       `json:"partition"`
// 	Offset    int64       `json:"offset"`
// 	Key       string      `json:"key"`
// 	Value     interface{} `json:"value"`
// }

type Res struct {
	Code         string `json:"code"`
	ReceivedTime string `json:"received_time"`
}

func main() {

	// Echo instance
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	// Routes
	e.POST("/getMessage", func(c echo.Context) error {
		var req Req
		if err := c.Bind(&req); err != nil {
			return err
		}
		// convert body into bytes and send it to kafka
		msgInBytes, err := json.Marshal(req)
		if err != nil {
			fmt.Println(err)
		}
		ProduceMsg("test", []byte(msgInBytes))
		t := time.Now()
		res := Res{
			Code:         "OK",
			ReceivedTime: t.Format("2006-01-02 15:04:05"),
		}
		return c.JSON(http.StatusOK, res)
	})
	// Start server
	e.Logger.Fatal(e.Start(":1323"))

}

// setupProducer will create a AsyncProducer and returns it
func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func ProduceMsg(topic string, message []byte) error {

	brokersUrl := []string{"localhost:9092", "kafka:9093"}
	producer, err := ConnectProducer(brokersUrl)
	if err != nil {
		return err
	}

	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)

	return nil
}
