package initialize

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
)

var Broker = []string{"kafka_ip:9092"}

func NewProducer() sarama.SyncProducer {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(Broker, config)
	if err != nil  {
		fmt.Println(err)
	}
	return producer
}

func NewConsumer() sarama.Consumer{

	config := sarama.NewConfig()
	consumer, _ := sarama.NewConsumer(Broker, config)
	return consumer
}

func NewCassandraClient() *gocql.Session {

	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "test"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()

	return session
}


