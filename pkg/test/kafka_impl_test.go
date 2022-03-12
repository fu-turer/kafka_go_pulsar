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
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	partition       = 0
	testClientId    = "consumer-test-client-id"
	testContent     = "test-content"
	addr            = Addr{ip: "localhost", port: "10012", protocol: "tcp"}
	maxFetchWaitMs  = 1000
	maxFetchRecord  = 1
	pulsarClient, _ = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	config          = &kafsar.Config{
		KafsarConfig: kafsar.KafsarConfig{
			MaxFetchWaitMs:  maxFetchWaitMs,
			MaxFetchRecord:  maxFetchRecord,
			NamespacePrefix: "public/default",
		},
	}
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
	k := kafsar.NewKafsar(nil, config)
	err := k.InitGroupCoordinator()
	assert.Nil(t, err)
	err = k.ConnPulsar()
	assert.Nil(t, err)
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
	k := kafsar.NewKafsar(nil, config)
	err := k.InitGroupCoordinator()
	assert.Nil(t, err)
	err = k.ConnPulsar()
	assert.Nil(t, err)
	producer, err := pulsarClient.CreateProducer(pulsar.ProducerOptions{Topic: topic})
	assert.Nil(t, err)
	messageId, err := producer.Send(context.TODO(), &pulsar.ProducerMessage{Value: testContent})
	assert.Nil(t, err)

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
