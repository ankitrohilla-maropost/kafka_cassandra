package initialize

import (
	"github.com/Shopify/sarama"
	"github.com/gocql/gocql"
)

var Broker = []string{"127.0.0.1"}

func NewProducer() sarama.SyncProducer {

	config := sarama.NewConfig()
	producer, _ := sarama.NewSyncProducer(Broker, config)
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


