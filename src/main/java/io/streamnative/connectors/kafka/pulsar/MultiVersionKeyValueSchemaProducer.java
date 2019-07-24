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

import io.streamnative.connectors.kafka.KafkaAvroSchema;
import io.streamnative.connectors.kafka.KafkaSchemaAndBytes;
import io.streamnative.connectors.kafka.KafkaSchemaManager;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;

/**
 * Producer to handle multiple versions in key/value schemas.
 */
public class MultiVersionKeyValueSchemaProducer implements PulsarProducer {

    private final Schema keySchema;
    private final boolean isKeyKafkaAvroSchema;
    private final Schema valueSchema;
    private final boolean isValueKafkaAvroSchema;
    private final ConcurrentMap<KeyValue<Integer, Integer>, PulsarProducer> producers;
    private final PulsarClient client;
    private final String topic;
    private final Map<String, Object> producerConfig;
    private final MessageRouter messageRouter;
    private final KafkaSchemaManager schemaManager;

    public MultiVersionKeyValueSchemaProducer(PulsarClient client,
                                              String topic,
                                              Schema keySchema,
                                              Schema valueSchema,
                                              Map<String, Object> producerConfig,
                                              MessageRouter messageRouter,
                                              KafkaSchemaManager manager) {
        this.producers = new ConcurrentHashMap<>();
        this.keySchema = keySchema;
        this.isKeyKafkaAvroSchema = keySchema instanceof KafkaAvroSchema;
        this.valueSchema = valueSchema;
        this.isValueKafkaAvroSchema = valueSchema instanceof KafkaAvroSchema;
        this.client = client;
        this.topic = topic;
        this.producerConfig = producerConfig;
        this.messageRouter = messageRouter;
        this.schemaManager = manager;
    }

    @Override
    public CompletableFuture<MessageId> send(ConsumerRecord<Object, Object> record) {
        Object key = record.key();
        Object value = record.value();

        Integer keySchemaId = null;
        if (isKeyKafkaAvroSchema) {
            keySchemaId = ((KafkaSchemaAndBytes) key).getSchemaId();
        }
        Integer valueSchemaId = null;
        if (isValueKafkaAvroSchema) {
            valueSchemaId = ((KafkaSchemaAndBytes) value).getSchemaId();
        }

        return producers.computeIfAbsent(
            new KeyValue<>(keySchemaId, valueSchemaId),
            kv -> {
                try {
                    return createPulsarProducer(kv);
                } catch (PulsarClientException e) {
                    throw new RuntimeException("Failed to create a pulsar producer for a given key/value schema", e);
                }
            }
        ).send(record);
    }

    private PulsarProducer createPulsarProducer(KeyValue<Integer, Integer> kvSchemaIds)
        throws PulsarClientException {
        Schema keySchema = this.keySchema;
        Schema valueSchema = this.valueSchema;

        if (isKeyKafkaAvroSchema) {
            KafkaAvroSchema keyAvroSchema = new KafkaAvroSchema();
            keyAvroSchema.setAvroSchema(schemaManager.getKafkaSchema(kvSchemaIds.getKey()));
            keySchema = keyAvroSchema;
        }
        if (isValueKafkaAvroSchema) {
            KafkaAvroSchema valueAvroSchema = new KafkaAvroSchema();
            valueAvroSchema.setAvroSchema(schemaManager.getKafkaSchema(kvSchemaIds.getValue()));
            valueSchema = valueAvroSchema;
        }

        return new KeyValueSchemaProducer(
            client,
            topic,
            keySchema,
            valueSchema,
            producerConfig,
            messageRouter
        );
    }

    @Override
    public void close() throws Exception {
        for (PulsarProducer producer : producers.values()) {
            producer.close();
        }
    }
}
