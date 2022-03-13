package test

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestKafkaClient(t *testing.T) {

	topic := "kafsar"
	kafkaServerAddr := "localhost:9092"
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaServerAddr},
		GroupID:        groupId,
		Topic:          topic,
		SessionTimeout: 1 * time.Second,
		MinBytes:       1,
		MaxBytes:       10e6,
		MaxWait:        100 * time.Millisecond,
	})
	defer reader.Close()

	message, err := reader.ReadMessage(context.Background())
	assert.Nil(t, err)
	logrus.Infof("poll msg %v", message)
	assert.Equal(t, testContent, string(message.Value))
}

func TestPulsarProducerSendMsg(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{Topic: "public/default/kafsar"})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}); err != nil {
			log.Fatal(err)
		} else {
			log.Println("Published message: ", msgId)
		}
	}
}
