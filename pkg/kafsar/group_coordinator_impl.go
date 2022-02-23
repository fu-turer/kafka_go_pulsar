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
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/sirupsen/logrus"
	"sync"
)

type GroupCoordinatorImpl struct {
	pulsarConfig PulsarConfig
	KafsarConfig KafsarConfig
	pulsarClient pulsar.Client
	mutex        sync.RWMutex
	GroupManager map[string]Group
}

type GroupStatus int

const (
	PreparingRebalance GroupStatus = 1 + iota
	CompletingRebalance
	Stable
	Dead
	Empty
)

type Group struct {
	groupId        string
	groupStatus    GroupStatus
	groupProtocols []*service.GroupProtocol
	protocolType   string
	members        map[string]memberMetadata
}

type memberMetadata struct {
	memberId string
	metadata []byte
}

func (gci *GroupCoordinatorImpl) HandleJoinGroup(groupId, memberId, clientId, protocolType string, sessionTimeoutMs int,
	protocols []*service.GroupProtocol) (*service.JoinGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("join group failed, cause invalid groupId")
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	if sessionTimeoutMs < gci.KafsarConfig.GroupMinSessionTimeoutMs || sessionTimeoutMs > gci.KafsarConfig.GroupMaxSessionTimeoutMs {
		logrus.Errorf("join group failed, cause invalid sessionTimeoutMs: %d. minSessionTimeoutMs: %d, maxSessionTimeoutMs: %d",
			sessionTimeoutMs, gci.KafsarConfig.GroupMinSessionTimeoutMs, gci.KafsarConfig.GroupMaxSessionTimeoutMs)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.INVALID_SESSION_TIMEOUT,
		}, nil
	}
	gci.mutex.RLock()
	group, exist := gci.GroupManager[groupId]
	gci.mutex.RUnlock()
	gci.mutex.Lock()
	defer gci.mutex.Unlock()
	if !exist {
		// reject if first member with empty Group protocol or protocolType is empty
		if protocolType == "" || len(protocols) == 0 {
			logrus.Errorf("join group failed, cause group protocols or protocolType empty. groupId: %s, memberId: %s", groupId, memberId)
			return &service.JoinGroupResp{
				MemberId:  memberId,
				ErrorCode: service.INCONSISTENT_GROUP_PROTOCOL,
			}, nil
		}
		group = Group{
			groupId:        groupId,
			groupStatus:    Empty,
			protocolType:   protocolType,
			groupProtocols: protocols,
			members:        make(map[string]memberMetadata),
		}
		gci.GroupManager[groupId] = group
	}
	members := group.members
	numMember := len(members)
	if numMember >= gci.KafsarConfig.MaxConsumersPerGroup {
		logrus.Errorf("join group failed, exceed maximum number of group. groupId: %s, memberId: %s, current: %d, maxConsumersPerGroup: %d",
			groupId, memberId, numMember, gci.KafsarConfig.MaxConsumersPerGroup)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}

	if group.groupStatus == Empty && memberId == "" {
		memberId = clientId + "-" + uuid.New().String()
	}
	if group.groupStatus == Dead {
		logrus.Errorf("join group failed, cause group has been removed. groupId: %s, memberId: %s", groupId, memberId)
		return &service.JoinGroupResp{
			MemberId:  memberId,
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}
	// TODO multi-consumer joinGroup PreparingRebalance
	// TODO multi-consumer joinGroup CompletingRebalance

	if group.groupStatus == Empty || group.groupStatus == Stable {
		protocol := group.groupProtocols[0]
		protocolName := protocol.ProtocolName
		protocolMetadata := protocol.ProtocolMetadata
		members[memberId] = memberMetadata{memberId: memberId, metadata: []byte(protocolMetadata)}
		member := service.Member{
			MemberId:        memberId,
			GroupInstanceId: nil,
			Metadata:        protocolMetadata,
		}
		respMembers := make([]*service.Member, 1)
		respMembers[0] = &member
		group.groupStatus = Stable
		logrus.Infof("success join group: %s, memberId: %s", groupId, memberId)
		return &service.JoinGroupResp{
			ErrorCode:    service.NONE,
			GenerationId: 0,
			ProtocolType: &protocolType,
			ProtocolName: protocolName,
			LeaderId:     memberId,
			MemberId:     memberId,
			Members:      respMembers,
		}, nil
	}
	return &service.JoinGroupResp{
		MemberId:  memberId,
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (gci *GroupCoordinatorImpl) HandleSyncGroup(groupId, memberId string, generation int,
	groupAssignments []*service.GroupAssignment) (*service.SyncGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("sync group failed, cause groupId is empty")
		return &service.SyncGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	// reject if memberId is empty
	if memberId == "" {
		logrus.Errorf("sync group failed, cause memberId is empty")
		return &service.SyncGroupResp{
			ErrorCode: service.MEMBER_ID_REQUIRED,
		}, nil
	}
	gci.mutex.RLock()
	groupMeta, exist := gci.GroupManager[groupId]
	gci.mutex.RUnlock()
	if !exist {
		logrus.Errorf("sync group failed, cause invalid groupId")
		return &service.SyncGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	_, exist = groupMeta.members[memberId]
	if !exist {
		logrus.Errorf("sync group failed, cause invalid memberId")
		return &service.SyncGroupResp{
			ErrorCode: service.UNKNOWN_MEMBER_ID,
		}, nil
	}
	for i := range groupAssignments {
		if groupAssignments[i].MemberId == memberId {
			logrus.Infof("success sync group: %s, memberId: %s", groupId, memberId)
			return &service.SyncGroupResp{
				ErrorCode:        service.NONE,
				MemberAssignment: groupAssignments[i].MemberAssignment,
			}, nil
		}
	}
	return &service.SyncGroupResp{
		ErrorCode: service.UNKNOWN_SERVER_ERROR,
	}, nil
}

func (gci *GroupCoordinatorImpl) HandleLeaveGroup(groupId string,
	members []*service.LeaveGroupMember) (*service.LeaveGroupResp, error) {
	// reject if groupId is empty
	if groupId == "" {
		logrus.Errorf("leave group failed, cause groupId is empty")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	gci.mutex.RLock()
	groupMeta, exist := gci.GroupManager[groupId]
	gci.mutex.RUnlock()
	if !exist {
		logrus.Errorf("leave group failed, cause group not exist")
		return &service.LeaveGroupResp{
			ErrorCode: service.INVALID_GROUP_ID,
		}, nil
	}
	membersMeta := groupMeta.members
	for i := range members {
		delete(membersMeta, members[i].MemberId)
		logrus.Infof("consumer member: %s success leave group: %s", members[i].MemberId, groupId)
	}
	return &service.LeaveGroupResp{ErrorCode: service.NONE, Members: members}, nil
}
