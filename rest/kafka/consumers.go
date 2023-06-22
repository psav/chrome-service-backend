package kafka

import (
	"github.com/RedHatInsights/chrome-service-backend/config"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type kafkaConsumer struct {
	Topics  []string
	Readers map[string]*kafka.Reader
}

var KafkaConsumer = kafkaConsumer{}

const TEN_MB = 10e7

func CreateReader(topic string) *kafka.Reader {
	cfg := config.Get()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.KafkaConfig.KafkaBrokers,
		// Move this to a dynamic id based on some facet of the websocket connection
		// Then, make sure you close it when the connection terminates
		GroupID:     "platform.chrome",
		StartOffset: kafka.LastOffset,
		Topic:       topic,
		Logger:      kafka.LoggerFunc(logrus.Debugf),
		ErrorLogger: kafka.LoggerFunc(logrus.Errorf),
		MaxBytes:    TEN_MB,
	})
	logrus.Infoln("Creating new kafka reader for topic:", topic)
	return r
}

func InitializeConsumers() {
	cfg := config.Get()
	topics := cfg.KafkaConfig.KafkaTopics
	readers := make(map[string]*kafka.Reader)
	for _, topic := range topics {
		readers[topic] = CreateReader(topic)
	}

	KafkaConsumer.Readers = readers

	//for _, r := range readers {
	//	go StartKafkaReader(r)
	//}
}
