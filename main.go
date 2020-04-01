package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
	"kafka_cassandra/initialize"
	"log"
	"time"
)

var Producer sarama.SyncProducer
var Consumer sarama.Consumer
var Session *gocql.Session

func init() {

	Producer = initialize.NewProducer()
	Consumer = initialize.NewConsumer()
	Session = initialize.NewCassandraClient()
}

func main() {

	go sendSampleMsg()
	//commandMsgChan := make(chan *sarama.ConsumerMessage, 1000)
	startConsumer()
}

func startConsumer() {

	consumer, err := Consumer.ConsumePartition("data", 0, sarama.OffsetNewest)
	if err != nil {
		fmt.Println("Unable to consume:", err)
	}

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				var text string
				fmt.Println(string(msg.Value))

				// insert into cassandra
				if err := Session.Query(`INSERT INTO test_tb (text) VALUES (?)`,
					string(msg.Value)).Exec(); err != nil {
					log.Fatal(err)
				}
				iter := Session.Query(`SELECT text FROM test_tb`).Iter()
				for iter.Scan(&text) {
					fmt.Println("test_tb content:", text)
				}
			}
		}
	}()
}

func sendSampleMsg() {
	data := "test_data"
	saramaMsg := sarama.ProducerMessage{
		Topic:     "data",
		Key:       sarama.StringEncoder("test_id"),
		Value:     sarama.StringEncoder(data),
		Timestamp: time.Now(),
	}
	_, _, err := Producer.SendMessage(&saramaMsg)
	if err != nil {
		fmt.Println("Unable to send:", err)
	}
}


