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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * Producer for key without schema and value with schema.
 */
@Slf4j
@SuppressWarnings("unchecked")
public class MultiVersionRawKeySchemaValueProducer implements PulsarProducer {

    private final KafkaSchemaManager schemaManager;
    private final boolean isValueKafkaAvroSchema;
    private final Schema valueSchema;
    private final ConcurrentMap<Integer, PulsarProducer> producers;
    private final PulsarClient client;
    private final String topic;
    private final Map<String, Object> producerConfig;
    private final MessageRouter messageRouter;
    private final Optional<PulsarProducer> nullSchemaIdProducer;

    public MultiVersionRawKeySchemaValueProducer(PulsarClient client,
                                                 String topic,
                                                 Schema valueSchema,
                                                 Map<String, Object> producerConfig,
                                                 MessageRouter messageRouter) throws PulsarClientException {
        this(client, topic, valueSchema, producerConfig, messageRouter, null);
    }

    public MultiVersionRawKeySchemaValueProducer(PulsarClient client,
                                                 String topic,
                                                 Schema valueSchema,
                                                 Map<String, Object> producerConfig,
                                                 MessageRouter messageRouter,
                                                 KafkaSchemaManager manager) throws PulsarClientException {
        this.schemaManager = manager;
        this.valueSchema = valueSchema;
        this.isValueKafkaAvroSchema = valueSchema instanceof KafkaAvroSchema;
        this.producers = new ConcurrentHashMap<>();
        this.client = client;
        this.topic = topic;
        this.producerConfig = producerConfig;
        this.messageRouter = messageRouter;
        if (this.isValueKafkaAvroSchema) {
            this.nullSchemaIdProducer = Optional.empty();
        } else {
            this.nullSchemaIdProducer = Optional.of(createPulsarProducer(null));
        }
    }

    @Override
    public CompletableFuture<MessageId> send(ConsumerRecord<Object, Object> record) {
        Object recordValue = record.value();

        final Integer valueSchemaId;
        if (isValueKafkaAvroSchema) {
            valueSchemaId = ((KafkaSchemaAndBytes) recordValue).getSchemaId();
        } else {
            valueSchemaId = null;
        }

        return nullSchemaIdProducer.orElseGet(
            () -> producers.computeIfAbsent(
                valueSchemaId,
                schemaId -> {
                    try {
                        return createPulsarProducer(valueSchemaId);
                    } catch (PulsarClientException e) {
                        throw new RuntimeException(
                            "Failed to create pulsar producer for a given schema " + valueSchemaId, e);
                    }
                }
            )
        ).send(record);
    }

    private PulsarProducer createPulsarProducer(Integer valueSchemaId)
        throws PulsarClientException {
        Schema valueSchema = this.valueSchema;

        if (isValueKafkaAvroSchema) {
            KafkaAvroSchema valueAvroSchema = new KafkaAvroSchema();
            valueAvroSchema.setAvroSchema(schemaManager.getKafkaSchema(valueSchemaId));
            valueSchema = valueAvroSchema;
        }

        try {
            return new RawKeySchemaValueProducer(
                client,
                topic,
                valueSchema,
                producerConfig,
                messageRouter
            );
        } catch (PulsarClientException e) {
            log.error("Failed to create a producer with value schema = {}",
                valueSchema.getSchemaInfo(),
                e);
            throw e;
        }
    }


    @Override
    public void close() throws Exception {
        for (PulsarProducer producer : producers.values()) {
            producer.close();
        }
    }
}
