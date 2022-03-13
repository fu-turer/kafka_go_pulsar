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
	"github.com/paashzj/kafka_go/pkg/kafka"
	"github.com/paashzj/kafka_go_pulsar/pkg/kafsar"
)

func setupKafsar() (*kafsar.Broker, int) {
	port, err := AcquireUnusedPort()
	if err != nil {
		panic(err)
	}
	broker, err := setupKafsarInternal(port)
	if err != nil {
		panic(err)
	}
	return broker, port
}

func setupKafsarInternal(port int) (*kafsar.Broker, error) {
	config := &kafsar.Config{}
	config.KafkaConfig = kafka.ServerConfig{}
	config.KafkaConfig.ListenHost = "localhost"
	config.KafkaConfig.ListenPort = port
	config.KafkaConfig.AdvertiseHost = "localhost"
	config.KafkaConfig.AdvertisePort = port
	config.PulsarConfig = kafsar.PulsarConfig{}
	config.PulsarConfig.Host = "localhost"
	config.PulsarConfig.HttpPort = 8080
	config.PulsarConfig.TcpPort = 6650
	config.KafsarConfig.MaxConsumersPerGroup = 100
	config.KafsarConfig.GroupMaxSessionTimeoutMs = 60000
	config.KafsarConfig.GroupMinSessionTimeoutMs = 0
	config.KafsarConfig.MaxFetchRecord = 10
	config.KafsarConfig.MaxFetchWaitMs = 100
	config.KafsarConfig.NamespacePrefix = "public/default"
	kafsarImpl := &KafsarImpl{}
	return kafsar.Run(config, kafsarImpl)
}
