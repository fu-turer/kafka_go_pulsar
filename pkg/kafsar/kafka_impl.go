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

package kafsar

import (
	"container/list"
	"encoding/json"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/sirupsen/logrus"
	"net"
	"strconv"
	"sync"
	"time"
)

type KafkaImpl struct {
	server           Server
	pulsarConfig     PulsarConfig
	pulsarClient     pulsar.Client
	groupCoordinator *GroupCoordinatorImpl
	kafsarConfig     KafsarConfig
	consumerManager  map[string]*ConsumerMetadata
	mutex            sync.RWMutex
}

func NewKafsar(impl Server, config *Config) *KafkaImpl {
	kafka := KafkaImpl{server: impl, pulsarConfig: config.PulsarConfig, kafsarConfig: config.KafsarConfig}
	kafka.consumerManager = make(map[string]*ConsumerMetadata)
	return &kafka
}

func (k *KafkaImpl) InitGroupCoordinator() (err error) {
	k.groupCoordinator = NewGroupCoordinator(k.pulsarConfig, k.kafsarConfig, k.pulsarClient)
	return
}

func (k *KafkaImpl) Produce(addr net.Addr, topic string, partition int, req *service.ProducePartitionReq) (*service.ProducePartitionResp, error) {
	panic("implement me")
}

func (k *KafkaImpl) ConnPulsar() (err error) {
	k.pulsarClient, err = pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	return
}

func (k *KafkaImpl) FetchPartition(addr net.Addr, topic string, req *service.FetchPartitionReq) (*service.FetchPartitionResp, error) {
	logrus.Infof("%s fetch topic: %s", addr.String(), topic)
	//TODO topic authorization

	fullNameTopic := k.kafsarConfig.NamespacePrefix + "/" + topic
	k.mutex.RLock()
	consumerMetadata, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("can not find consumer for topic: %s when fetch partition", fullNameTopic)
		return &service.FetchPartitionResp{
			PartitionId: req.PartitionId,
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	var records []*service.Record
	recordBatch := service.RecordBatch{Records: records}
	fetchStart := time.Now()
	for ;; {
		timepicker := make(chan bool, 1)
		go func() {
			time.Sleep(1 * time.Millisecond)
			timepicker <- true
		}()
		select {
		case channel := <-consumerMetadata.channel:
			message := channel.Message
			logrus.Infof("receive msg: %s from %s", message.ID(), message.Topic())
			_, _ = json.Marshal(message.Properties())
			offset := ConvOffset(message.ID())
			record := service.Record{
				Value:          message.Payload(),
				RelativeOffset: offset,
			}
			recordBatch.Records = append(recordBatch.Records, &record)
			recordBatch.Offset = int64(offset)
			consumerMetadata.messageIds.PushBack(message.ID())
		case <-timepicker:
			logrus.Debugf("fetch empty message for topic %s", fullNameTopic)
		}
		if time.Since(fetchStart).Milliseconds() >= int64(k.kafsarConfig.MaxFetchWaitMs) || len(recordBatch.Records) >= k.kafsarConfig.MaxFetchRecord {
			break
		}
	}
	return &service.FetchPartitionResp{
		ErrorCode:        service.NONE,
		PartitionId:      req.PartitionId,
		LastStableOffset: recordBatch.Offset,
		LogStartOffset:   recordBatch.Offset,
		RecordBatch:      &recordBatch,
	}, nil
}

func (k *KafkaImpl) GroupJoin(addr net.Addr, req *service.JoinGroupReq) (*service.JoinGroupResp, error) {
	logrus.Infof("%s joining to group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	joinGroupResp, err := k.groupCoordinator.HandleJoinGroup(req.GroupId, req.MemberId, req.ClientId, req.ProtocolType,
		req.SessionTimeout, req.GroupProtocols)
	if err != nil {
		logrus.Errorf("unexpected exception in join group: %s, error: %s", req.GroupId, err)
		return &service.JoinGroupResp{
			ErrorCode:    service.UNKNOWN_SERVER_ERROR,
			MemberId:     req.MemberId,
			GenerationId: -1,
		}, nil
	}
	return joinGroupResp, nil
}

func (k *KafkaImpl) GroupLeave(addr net.Addr, req *service.LeaveGroupReq) (*service.LeaveGroupResp, error) {
	logrus.Infof("%s leaving group: %s, members: %+v", addr.String(), req.GroupId, req.Members)
	leaveGroupResp, err := k.groupCoordinator.HandleLeaveGroup(req.GroupId, req.Members)
	if err != nil {
		logrus.Errorf("unexpected exception in leaving group: %s, error: %s", req.GroupId, err)
		return &service.LeaveGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return leaveGroupResp, nil
}

func (k *KafkaImpl) GroupSync(addr net.Addr, req *service.SyncGroupReq) (*service.SyncGroupResp, error) {
	logrus.Infof("%s syncing group: %s, memberId: %s", addr.String(), req.GroupId, req.MemberId)
	syncGroupResp, err := k.groupCoordinator.HandleSyncGroup(req.GroupId, req.MemberId, req.GenerationId, req.GroupAssignments)
	if err != nil {
		logrus.Errorf("unexpected exception in sync group: %s, error: %s", req.GroupId, err)
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_SERVER_ERROR,
		}, nil
	}
	return syncGroupResp, nil
}

func (k *KafkaImpl) OffsetListPartition(addr net.Addr, topic string, req *service.ListOffsetsPartitionReq) (*service.ListOffsetsPartitionResp, error) {
	logrus.Infof("%s offset list topic: %s, partition: %d", addr.String(), topic, req.PartitionId)
	return &service.ListOffsetsPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      0,
	}, nil
}

func (k *KafkaImpl) OffsetCommitPartition(addr net.Addr, topic string, req *service.OffsetCommitPartitionReq) (*service.OffsetCommitPartitionResp, error) {
	logrus.Infof("%s topic: %s, partition: %d, commit offset: %d", addr.String(), topic, req.PartitionId, req.OffsetCommitOffset)
	fullNameTopic := k.kafsarConfig.NamespacePrefix + "/" + topic
	k.mutex.RLock()
	consumerMessages, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		logrus.Errorf("commit offset failed, topic: %s, does not exist", fullNameTopic)
		return &service.OffsetCommitPartitionResp{ErrorCode: service.UNKNOWN_TOPIC_ID}, nil
	}
	ids := consumerMessages.messageIds
	front := ids.Front()
	for element := front; element != nil; element = element.Next() {
		messageId := element.Value.(pulsar.MessageID)
		offset := ConvOffset(messageId)
		if int64(offset) > req.OffsetCommitOffset {
			break
		}
		logrus.Infof("ack pulsar %s for %s", fullNameTopic, messageId)
		consumerMessages.consumer.AckID(messageId)
		consumerMessages.messageIds.Remove(element)
	}
	return &service.OffsetCommitPartitionResp{
		PartitionId: req.PartitionId,
		ErrorCode:   service.NONE,
	}, nil
}

func (k *KafkaImpl) OffsetFetch(addr net.Addr, topic string, req *service.OffsetFetchPartitionReq) (*service.OffsetFetchPartitionResp, error) {
	logrus.Infof("%s fetch topic: %s offset, partition: %d", addr.String(), topic, req.PartitionId)
	fullNameTopic := k.kafsarConfig.NamespacePrefix + "/" + topic
	groupId, err := k.server.SyncGroup(req.GroupId)
	if err != nil {
		logrus.Errorf("sync group %s failed when offset fetch, error: %s", req.GroupId, err)
	}
	k.mutex.RLock()
	consumerMetadata, exist := k.consumerManager[fullNameTopic]
	k.mutex.RUnlock()
	if !exist {
		k.mutex.Lock()
		metadata := ConsumerMetadata{groupId: groupId, messageIds: list.New()}
		channel, consumer, err := k.createConsumer(topic, req.GroupId)
		if err != nil {
			logrus.Errorf("%s, create channel failed, error: %s", topic, err)
			return &service.OffsetFetchPartitionResp{
				ErrorCode: int16(service.UNKNOWN_SERVER_ERROR),
			}, nil
		}
		metadata.consumer = consumer
		metadata.channel = channel
		k.consumerManager[fullNameTopic] = &metadata
		consumerMetadata = &metadata
		k.mutex.Unlock()
	}
	k.mutex.RLock()
	group := k.groupCoordinator.groupManager[req.GroupId]
	k.mutex.RUnlock()
	group.topic = fullNameTopic
	group.consumerMetadata = consumerMetadata
	return &service.OffsetFetchPartitionResp{
		PartitionId: req.PartitionId,
		Offset:      0,
		LeaderEpoch: -1,
		Metadata:    nil,
		ErrorCode:   int16(service.NONE),
	}, nil
}

func (k *KafkaImpl) SaslAuth(req service.SaslReq) (bool, service.ErrorCode) {
	auth, err := k.server.Auth(req.Username, req.Password, req.ClientId)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthTopic(req service.SaslReq, topic string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopic(req.Username, req.Password, req.ClientId, topic)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) SaslAuthConsumerGroup(req service.SaslReq, consumerGroup string) (bool, service.ErrorCode) {
	auth, err := k.server.AuthTopicGroup(req.Username, req.Password, req.ClientId, consumerGroup)
	if err != nil || !auth {
		return false, service.SASL_AUTHENTICATION_FAILED
	}
	return true, service.NONE
}

func (k *KafkaImpl) Disconnect(addr net.Addr) {
	panic("implement me")
}

func (k *KafkaImpl) createConsumer(topic, groupId string) (chan pulsar.ConsumerMessage, pulsar.Consumer, error) {
	channel := make(chan pulsar.ConsumerMessage, k.kafsarConfig.ConsumerReceiveQueueSize)
	options := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            groupId,
		Type:                        pulsar.Failover,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		MessageChannel:              channel,
		ReceiverQueueSize:           k.kafsarConfig.ConsumerReceiveQueueSize,
	}
	consumer, err := k.pulsarClient.Subscribe(options)
	if err != nil {
		return nil, nil, err
	}
	return channel, consumer, nil
}

func ConvOffset(id pulsar.MessageID) int {
	offset, _ := strconv.Atoi(fmt.Sprint(id.LedgerID()) + fmt.Sprint(id.EntryID()) + fmt.Sprint(id.PartitionIdx()))
	return offset
}
