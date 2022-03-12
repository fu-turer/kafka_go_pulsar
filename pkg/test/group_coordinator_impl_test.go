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
	"github.com/paashzj/kafka_go/pkg/service"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	clientId         = "consumer-a1e12365-ddfa-43fc-826e-9661fb54c274-1"
	groupId          = "test-group-id"
	memberId         = "test-member-id"
	sessionTimeoutMs = 30000
	protocolType     = "consumer"
	protocol         = service.GroupProtocol{
		ProtocolName:     "range",
		ProtocolMetadata: "000100000001000474657374ffffffff00000000",
	}
	groupProtocol []*service.GroupProtocol
	protocols     = append(groupProtocol, &protocol)
	generation    = 1
	assignment    = service.GroupAssignment{
		MemberId:         memberId,
		MemberAssignment: "0001000000010004746573740000000100000000ffffffff",
	}
	groupAssignment  []*service.GroupAssignment
	groupAssignments = append(groupAssignment, &assignment)

	kafsarConfig = kafsar.KafsarConfig{
		MaxConsumersPerGroup:     1,
		GroupMinSessionTimeoutMs: 0,
		GroupMaxSessionTimeoutMs: 30000,
	}
)

func TestHandleJoinGroup(t *testing.T) {
	groupCoordinator := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)

	resp, err = groupCoordinator.HandleJoinGroup("test-group-id-2", memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)
}

func TestHandleJoinGroupMultiMember(t *testing.T) {
	groupCoordinator := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)

	newMemberId := "test-member-id-2"
	resp, err = groupCoordinator.HandleJoinGroup(groupId, newMemberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.UNKNOWN_SERVER_ERROR, resp.ErrorCode)
}

func TestHandleJoinGroupInvalidParams(t *testing.T) {
	// invalid groupId
	groupCoordinatorEmptyGroupId := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	groupIdEmpty := ""
	resp, err := groupCoordinatorEmptyGroupId.HandleJoinGroup(groupIdEmpty, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.INVALID_GROUP_ID, resp.ErrorCode)

	// invalid protocol
	groupCoordinatorEmptyProtocol := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	var protocolsEmpty []*service.GroupProtocol
	resp, err = groupCoordinatorEmptyProtocol.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocolsEmpty)
	assert.Nil(t, err)
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
	groupCoordinatorEmptyProtocolType := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	protocolTypeEmpty := ""
	resp, err = groupCoordinatorEmptyProtocolType.HandleJoinGroup(groupId, memberId, clientId, protocolTypeEmpty, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.INCONSISTENT_GROUP_PROTOCOL, resp.ErrorCode)
}

func TestHandleSyncGroup(t *testing.T) {
	groupCoordinator := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	syncGroupResp, err := groupCoordinator.HandleSyncGroup(groupId, memberId, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, syncGroupResp.ErrorCode)
}

func TestHandleSyncGroupInvalidParams(t *testing.T) {
	groupCoordinator := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	joinGroupResp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, joinGroupResp.ErrorCode)

	// invalid groupId
	groupIdEmpty := ""
	syncGroupResp, err := groupCoordinator.HandleSyncGroup(groupIdEmpty, memberId, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.INVALID_GROUP_ID, syncGroupResp.ErrorCode)

	// invalid memberId
	memberIdInvalid := "test-member-id-invalid"
	syncGroupResp, err = groupCoordinator.HandleSyncGroup(groupId, memberIdInvalid, generation, groupAssignments)
	assert.Nil(t, err)
	assert.Equal(t, service.UNKNOWN_MEMBER_ID, syncGroupResp.ErrorCode)
}

func TestLeaveGroup(t *testing.T) {
	groupCoordinator := kafsar.GroupCoordinatorImpl{GroupManager: make(map[string]kafsar.Group), KafsarConfig: kafsarConfig}
	resp, err := groupCoordinator.HandleJoinGroup(groupId, memberId, clientId, protocolType, sessionTimeoutMs, protocols)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, resp.ErrorCode)

	var members []*service.LeaveGroupMember
	leaveGroupMembers := append(members, &service.LeaveGroupMember{
		MemberId: memberId,
	})
	leaveGroupResp, err := groupCoordinator.HandleLeaveGroup(groupId, leaveGroupMembers)
	assert.Nil(t, err)
	assert.Equal(t, service.NONE, leaveGroupResp.ErrorCode)
}
