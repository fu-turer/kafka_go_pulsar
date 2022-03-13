// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package test

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var (
	partition       = 0
	testClientId    = "consumer-test-client-id"
	testContent     = "test-content"
	addr            = Addr{ip: "localhost", port: "10012", protocol: "tcp"}
	maxFetchWaitMs  = 500
	maxFetchRecord  = 1
	pulsarClient, _ = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	config          = &kafsar.Config{
		KafsarConfig: kafsar.KafsarConfig{
			MaxConsumersPerGroup: 1,
			GroupMinSessionTimeoutMs: 0,
			GroupMaxSessionTimeoutMs: 30000,
			MaxFetchWaitMs:  maxFetchWaitMs,
			MaxFetchRecord:  maxFetchRecord,
			NamespacePrefix: "public/default",
		},
	}
	kafsarServer = KafsarImpl{}
)

type Addr struct {
	ip       string
	port     string
	protocol string
}

func (a *Addr) Network() string {
	return a.protocol
}
func (a *Addr) String() string {
	return a.ip + ":" + a.port
}

func TestFetchPartitionNoMessage(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	setupPulsar()
	k := kafsar.NewKafsar(kafsarServer, config)
	err := k.InitGroupCoordinator()
	assert.Nil(t, err)
	err = k.ConnPulsar()
	assert.Nil(t, err)

	// join group
	joinGroupReq := service.JoinGroupReq{
		ClientId:       clientId,
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offset, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	assert.Nil(t, err)
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offset.Offset,
		ClientId:    testClientId,
	}
	_, err = k.FetchPartition(&addr, topic, &fetchPartitionReq)
	assert.Nil(t, err)
}

func TestFetchAndCommitOffset(t *testing.T) {
	topic := uuid.New().String()
	groupId := uuid.New().String()
	setupPulsar()
	k := kafsar.NewKafsar(kafsarServer, config)
	err := k.InitGroupCoordinator()
	assert.Nil(t, err)
	err = k.ConnPulsar()
	assert.Nil(t, err)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	messageId, err := producer.Send(context.TODO(), &pulsar.ProducerMessage{Value: testContent})
	logrus.Infof("send msg to pulsar %s", messageId)
	assert.Nil(t, err)

	// join group
	joinGroupReq := service.JoinGroupReq{
		ClientId:       clientId,
		GroupId:        groupId,
		SessionTimeout: sessionTimeoutMs,
		ProtocolType:   protocolType,
		GroupProtocols: protocols,
	}
	joinGroupResp, err := k.GroupJoin(&addr, &joinGroupReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// offset fetch
	offsetFetchReq := service.OffsetFetchPartitionReq{
		GroupId:     groupId,
		ClientId:    testClientId,
		PartitionId: partition,
	}
	offsetFetchPartitionResp, err := k.OffsetFetch(&addr, topic, &offsetFetchReq)
	assert.Nil(t, err)
	assert.Equal(t, int16(service.NONE), offsetFetchPartitionResp.ErrorCode)

	// fetch partition
	fetchPartitionReq := service.FetchPartitionReq{
		PartitionId: partition,
		FetchOffset: offsetFetchPartitionResp.Offset,
		ClientId:    testClientId,
	}
	fetchPartitionResp, err := k.FetchPartition(&addr, topic, &fetchPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, fetchPartitionResp.ErrorCode)
	assert.Equal(t, maxFetchRecord, len(fetchPartitionResp.RecordBatch.Records))
	offset := fetchPartitionResp.RecordBatch.Records[0].RelativeOffset
	assert.Equal(t, kafsar.ConvOffset(messageId), offset)

	// offset commit
	offsetCommitPartitionReq := service.OffsetCommitPartitionReq{
		ClientId:           clientId,
		PartitionId:        partition,
		OffsetCommitOffset: int64(offset),
	}
	commitPartitionResp, err := k.OffsetCommitPartition(&addr, topic, &offsetCommitPartitionReq)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, commitPartitionResp.ErrorCode)
}

func TestConsumeByKafkaClient(t *testing.T) {
	setupPulsar()
	_, port := setupKafsar()
	topic := uuid.New().String()
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	_, err = producer.Send(context.TODO(), &pulsar.ProducerMessage{Value: testContent})
	assert.Nil(t, err)

	kafkaServerAddr := "localhost:" + strconv.Itoa(port)
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
	assert.Equal(t, testContent, string(message.Value))
}
