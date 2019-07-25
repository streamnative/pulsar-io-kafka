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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * Producer for key without schema and value with schema.
 */
@SuppressWarnings("unchecked")
public class RawKeySchemaValueProducer extends PulsarProducerBase {

    public RawKeySchemaValueProducer(PulsarClient client,
                                     String topic,
                                     Schema valueSchema,
                                     Map<String, Object> producerConfig,
                                     MessageRouter messageRouter) throws PulsarClientException {
        super(
            client,
            topic,
            valueSchema,
            producerConfig,
            messageRouter);
    }

    @Override
    public CompletableFuture<MessageId> send(ConsumerRecord<Object, Object> record) {
        TypedMessageBuilder<Object> msgBuilder = newMessage(record);
        Object recordKey = record.key();
        if (null != recordKey) {
            msgBuilder = msgBuilder.keyBytes((byte[]) recordKey);
        }
        Object recordValue = record.value();
        if (null != recordValue) {
            msgBuilder = msgBuilder.value(record.value());
        }
        return msgBuilder.sendAsync();
    }

}
