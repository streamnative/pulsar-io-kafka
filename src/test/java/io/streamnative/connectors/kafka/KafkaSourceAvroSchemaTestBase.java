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
package io.streamnative.connectors.kafka;

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_GENERIC_RECORD_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_OBJECT_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_VALUE_DECODER;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AvroValueVerifier;
import io.streamnative.tests.pulsar.service.PulsarService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.shade.org.glassfish.jersey.internal.util.Base64;

/**
 * Test base used for testing kafka avro source.
 */
@Slf4j
public abstract class KafkaSourceAvroSchemaTestBase extends KafkaSourceTestBase {

    protected static final String HEADER_TEST_SEQUENCE = "__test_sequence";

    protected final TopicNameStrategy nameStrategy;
    protected SchemaRegistryClient schemaRegistryClient;

    public KafkaSourceAvroSchemaTestBase(PulsarService service) {
        super(service);
        this.nameStrategy = new TopicNameStrategy();
    }

    @Override
    protected void doSetup() throws Exception {
        super.doSetup();

        this.schemaRegistryClient = new CachedSchemaRegistryClient(
            Lists.newArrayList(schemaRegistryServiceUri.getUri().toString()),
            1000,
            Collections.emptyMap()
        );
    }

    protected long registerTopicSchema(String topic, Schema schema, boolean isKey) throws Exception {
        String subjectName = nameStrategy.subjectName(topic, isKey, schema);
        return this.schemaRegistryClient.register(
            subjectName, schema
        );
    }

    protected void sendAvroMessagesToKafka(
        String kafkaTopic,
        int numPartitions, int numMessages
    ) throws Exception {
        KafkaAvroSerializer keySerializer = new KafkaAvroSerializer();
        KafkaAvroSerializer valueSerializer = new KafkaAvroSerializer();
        @Cleanup
        KafkaProducer<Object, Object> kafkaProducer = newKafkaProducer(
            keySerializer, valueSerializer
        );

        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                Object key = AVRO_GENERIC_RECORD_GENERATOR.apply(i, j);
                Object value = AVRO_GENERIC_RECORD_GENERATOR.apply(i, j);
                ProducerRecord<Object, Object> record = new ProducerRecord<>(
                    kafkaTopic, i,
                    (j + 1) * 1000L,
                    key,
                    value
                );
                record.headers().add(HEADER_TEST_SEQUENCE, Integer.toString(j).getBytes(UTF_8));
                CompletableFuture<RecordMetadata> sendFuture = new CompletableFuture<>();
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (null != exception) {
                        sendFuture.completeExceptionally(exception);
                    } else {
                        sendFuture.complete(metadata);
                    }
                }).get();
                RecordMetadata metadata = sendFuture.get();
                log.info("Send message to Kafka topic {} : ({}, {}) - offset: {}, timestamp: {}, key {}, value {}",
                    kafkaTopic, i, j, metadata.offset(), metadata.timestamp(), key, value);
            }
        }
        kafkaProducer.flush();
    }

    protected void receiveAvroValuesFromPulsar(
        Consumer<GenericRecord> valueConsumer,
        String pulsarTopic,
        int numPartitions, int numMessages,
        AvroValueVerifier<GenericRecord> valueVerifier
    ) throws Exception {
        Map<Integer, TreeMap<Integer, Message<GenericRecord>>> partitionIdxs = new HashMap<>();
        AtomicInteger totalReceived = new AtomicInteger(0);
        while (totalReceived.get() < numPartitions * numMessages) {
            Message<GenericRecord> message = valueConsumer.receive();
            TopicName tn = TopicName.get(message.getTopicName());
            assertEquals(
                TopicName.get(pulsarTopic).toString(),
                tn.getPartitionedTopicName()
            );
            int partitionIdx = tn.getPartitionIndex();
            int sequence = Integer.parseInt(Base64.decodeAsString(message.getProperty(HEADER_TEST_SEQUENCE)));

            log.info("Receive {} message from partition {} : total = {}",
                    partitionIdx, sequence, totalReceived.get());

            TreeMap<Integer, Message<GenericRecord>> partitionMap = partitionIdxs.computeIfAbsent(
                partitionIdx, p -> new TreeMap<>()
            );
            Message<GenericRecord> oldMessage = partitionMap.putIfAbsent(sequence, message);
            if (null == oldMessage) {
                totalReceived.incrementAndGet();
            } else {
                assertEquals(message.getKey(), oldMessage.getKey());
                assertEquals(message.getTopicName(), oldMessage.getTopicName());
                assertArrayEquals(message.getData(), oldMessage.getData());
                assertArrayEquals(message.getSchemaVersion(), oldMessage.getSchemaVersion());
                assertEquals(message.getEventTime(), oldMessage.getEventTime());
                assertEquals(message.getSequenceId(), oldMessage.getSequenceId());
                assertEquals(message.getProperties().size(), oldMessage.getProperties().size());
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                Map<Integer, Message<GenericRecord>> partitionMap = partitionIdxs.get(i);
                assertNotNull("No messages received for partition " + i, partitionMap);
                Message<GenericRecord> message = partitionMap.get(j);
                assertNotNull("No message found for (" + i + ", " + j + ")", message);

                assertEquals(
                    i,
                    Integer.parseInt(message.getProperty(KafkaSource.HEADER_KAFKA_PTN_KEY)));
                assertEquals(
                    pulsarTopic,
                    message.getProperty(KafkaSource.HEADER_KAFKA_TOPIC_KEY));
                assertEquals(
                    "Message received from partition (" + i + ", " + j + ") has wrong event time",
                    (j + 1) * 1000L, message.getEventTime());

                byte[] keyData = message.getKeyBytes();
                KafkaSchemaAndBytes ksBytes = KafkaSchemaAndBytesDeserializer.of().deserialize(
                    pulsarTopic,
                    keyData
                );
                byte[] keyBytes = new byte[ksBytes.getData().remaining()];
                ksBytes.getData().get(keyBytes);
                Object expectedKey = AVRO_OBJECT_GENERATOR.apply(i, j);
                Object actualKey = AVRO_VALUE_DECODER.decode(i, j, keyBytes);
                assertEquals(expectedKey, actualKey);

                GenericRecord actualRecord = message.getValue();
                valueVerifier.verify(i, j, actualRecord);
            }
        }
    }

    protected void receiveAvroKeyValuesFromPulsar(
        Consumer<KeyValue<GenericRecord, GenericRecord>> valueConsumer,
        String pulsarTopic,
        int numPartitions, int numMessages,
        AvroValueVerifier<GenericRecord> valueVerifier
    ) throws Exception {
        Map<Integer, TreeMap<Integer, Message<KeyValue<GenericRecord, GenericRecord>>>> partitionIdxs =
            new HashMap<>();
        AtomicInteger totalReceived = new AtomicInteger(0);
        while (totalReceived.get() < numPartitions * numMessages) {
            Message<KeyValue<GenericRecord, GenericRecord>> message = valueConsumer.receive();
            TopicName tn = TopicName.get(message.getTopicName());
            assertEquals(
                TopicName.get(pulsarTopic).toString(),
                tn.getPartitionedTopicName()
            );
            int partitionIdx = tn.getPartitionIndex();
            int sequence = Integer.parseInt(Base64.decodeAsString(message.getProperty(HEADER_TEST_SEQUENCE)));

            log.info("Receive {} message from partition {} : total = {}",
                    partitionIdx, sequence, totalReceived.get());

            TreeMap<Integer, Message<KeyValue<GenericRecord, GenericRecord>>> partitionMap =
                partitionIdxs.computeIfAbsent(partitionIdx, p -> new TreeMap<>());
            Message<KeyValue<GenericRecord, GenericRecord>> oldMessage = partitionMap.putIfAbsent(sequence, message);
            if (null == oldMessage) {
                totalReceived.incrementAndGet();
            } else {
                assertEquals(message.getKey(), oldMessage.getKey());
                assertEquals(message.getTopicName(), oldMessage.getTopicName());
                assertArrayEquals(message.getData(), oldMessage.getData());
                assertArrayEquals(message.getSchemaVersion(), oldMessage.getSchemaVersion());
                assertEquals(message.getEventTime(), oldMessage.getEventTime());
                assertEquals(message.getSequenceId(), oldMessage.getSequenceId());
                assertEquals(message.getProperties().size(), oldMessage.getProperties().size());
            }
        }

        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                Map<Integer, Message<KeyValue<GenericRecord, GenericRecord>>> partitionMap = partitionIdxs.get(i);
                assertNotNull("No messages received for partition " + i, partitionMap);
                Message<KeyValue<GenericRecord, GenericRecord>> message = partitionMap.get(j);
                assertNotNull("No message found for (" + i + ", " + j + ")", message);

                assertEquals(
                    i,
                    Integer.parseInt(message.getProperty(KafkaSource.HEADER_KAFKA_PTN_KEY)));
                assertEquals(
                    pulsarTopic,
                    message.getProperty(KafkaSource.HEADER_KAFKA_TOPIC_KEY));
                assertEquals(
                    "Message received from partition (" + i + ", " + j + ") has wrong event time",
                    (j + 1) * 1000L, message.getEventTime());

                GenericRecord actualKey = message.getValue().getKey();
                valueVerifier.verify(i, j, actualKey);
                GenericRecord actualValue = message.getValue().getValue();
                valueVerifier.verify(i, j, actualValue);
            }
        }
    }

}
