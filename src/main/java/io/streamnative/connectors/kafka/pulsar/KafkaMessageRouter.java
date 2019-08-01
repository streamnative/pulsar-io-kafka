/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.connectors.kafka.pulsar;

import static io.streamnative.connectors.kafka.KafkaSource.HEADER_KAFKA_PTN_KEY;

import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TopicMetadata;
import org.apache.pulsar.client.impl.RoundRobinPartitionMessageRouterImpl;

/**
 * A message router to route kafka messages to pulsar topic.
 */
public class KafkaMessageRouter extends RoundRobinPartitionMessageRouterImpl {

    public KafkaMessageRouter(HashingScheme hashingScheme,
                              int startPtnIdx,
                              boolean isBatchingEnabled,
                              long maxBatchingDelayMs) {
        super(hashingScheme, startPtnIdx, isBatchingEnabled, maxBatchingDelayMs);
    }

    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        String kafkaPartitionStr = msg.getProperties().get(HEADER_KAFKA_PTN_KEY);
        if (kafkaPartitionStr != null) {
            return Integer.parseInt(kafkaPartitionStr) % metadata.numPartitions();
        } else {
            return super.choosePartition(msg, metadata);
        }
    }

}
