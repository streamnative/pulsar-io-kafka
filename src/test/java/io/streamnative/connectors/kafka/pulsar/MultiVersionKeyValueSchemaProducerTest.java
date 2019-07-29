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

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.streamnative.connectors.kafka.KafkaMessageRouter;
import io.streamnative.connectors.kafka.KafkaSchemaManager;
import io.streamnative.connectors.kafka.KafkaSchemaManagerConfig;
import io.streamnative.connectors.kafka.schema.KafkaAvroSchema;
import io.streamnative.connectors.kafka.serde.KafkaSchemaAndBytes;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A system test to test producer with raw key and schema value.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(PulsarProducerTestSuite.class)
@Slf4j
public class MultiVersionKeyValueSchemaProducerTest extends PulsarProducerTestBase {

    private final SchemaRegistryClient schemaRegistry;
    private final KafkaSchemaManager schemaManager;

    public MultiVersionKeyValueSchemaProducerTest(PulsarService service) {
        super(service);
        this.schemaRegistry = new MockSchemaRegistryClient();
        HashMap<String, String> schemaManagerConfig = new HashMap<>();
        // Intentionally invalid schema registry URL to satisfy the config class's requirement that
        // it be set.
        schemaManagerConfig.put(KafkaSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        this.schemaManager = new KafkaSchemaManager(
            schemaRegistry,
            schemaManagerConfig
        );
    }

    private <K, V> void verifyPulsarMessage(Message<KeyValue<K, V>> pulsarMessage,
                                            int partition,
                                            int i,
                                            Generator<K> keyGenerator,
                                            Generator<V> valueGenerator) {
        assertEquals(i * 1000L, pulsarMessage.getSequenceId());
        assertEquals((i + 1) * 10000L, pulsarMessage.getEventTime());
        KeyValue<K, V> kv = pulsarMessage.getValue();
        assertValueEquals(partition, i, keyGenerator, kv.getKey());
        assertValueEquals(partition, i, valueGenerator, kv.getValue());
    }

    @Test
    public void testBytesSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.BYTES, Schema.BYTES,
            BYTES_GENERATOR, BYTES_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyBytesSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.BYTES, Schema.BYTES,
            null, BYTES_GENERATOR);
    }

    @Test
    public void testBooleanSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.BOOL, Schema.BOOL,
            BOOLEAN_GENERATOR, BOOLEAN_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyBooleanSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.BOOL, Schema.BOOL,
            null, BOOLEAN_GENERATOR);
    }

    @Test
    public void testInt8Schema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT8, Schema.INT8,
            BYTE_GENERATOR, BYTE_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyInt8Schema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT8, Schema.INT8,
            null, BYTE_GENERATOR);
    }

    @Test
    public void testInt16Schema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT16, Schema.INT16,
            SHORT_GENERATOR, SHORT_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyInt16Schema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT16, Schema.INT16,
            null, SHORT_GENERATOR);
    }

    @Test
    public void testInt32chema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT32, Schema.INT32,
            INTEGER_GENERATOR, INTEGER_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyInt32chema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT32, Schema.INT32,
            null, INTEGER_GENERATOR);
    }

    @Test
    public void testInt64chema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT64, Schema.INT64,
            LONG_GENERATOR, LONG_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyInt64chema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.INT64, Schema.INT64,
            null, LONG_GENERATOR);
    }

    @Test
    public void testFloatchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.FLOAT, Schema.FLOAT,
            FLOAT_GENERATOR, FLOAT_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyFloatchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.FLOAT, Schema.FLOAT,
            null, FLOAT_GENERATOR);
    }

    @Test
    public void testDoublechema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.DOUBLE, Schema.DOUBLE,
            DOUBLE_GENERATOR, DOUBLE_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyDoublechema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.DOUBLE, Schema.DOUBLE,
            null, DOUBLE_GENERATOR);
    }

    @Test
    public void testStringSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.STRING, Schema.STRING,
            STRING_GENERATOR, STRING_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyStringSchema() throws Exception {
        testSendMessages(
            10, 10,
            Schema.STRING, Schema.STRING,
            null, STRING_GENERATOR);
    }

    @Test
    public void testAvroSchema() throws Exception {
        Schema<User> schema = Schema.AVRO(
            SchemaDefinition.<User>builder()
                .withPojo(User.class)
                .withAlwaysAllowNull(false)
                .build());
        testSendMessages(
            10, 10,
            schema, schema,
            USER_GENERATOR, USER_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyAvroSchema() throws Exception {
        Schema<User> schema = Schema.AVRO(
            SchemaDefinition.<User>builder()
                .withPojo(User.class)
                .withAlwaysAllowNull(false)
                .build());
        testSendMessages(
            10, 10,
            schema, schema,
            null, USER_GENERATOR);
    }

    @Test
    public void testJsonSchema() throws Exception {
        Schema<User> schema = Schema.JSON(
            SchemaDefinition.<User>builder()
                .withPojo(User.class)
                .build());
        testSendMessages(
            10, 10,
            schema, schema,
            USER_GENERATOR, USER_GENERATOR);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4804")
    public void testNullKeyJsonSchema() throws Exception {
        Schema<User> schema = Schema.JSON(
            SchemaDefinition.<User>builder()
                .withPojo(User.class)
                .build());
        testSendMessages(
            10, 10,
            schema, schema,
            null, USER_GENERATOR);
    }

    private <K, V> void testSendMessages(
        int numPartitions, int numMessages,
        Schema<K> keySchema, Schema<V> valueSchema,
        Generator<K> keyGenerator,
        Generator<V> valueGenerator
    ) throws Exception {
        provisionPartitionedTopic(PUBLIC_TENANT, numPartitions, topicName -> {
            testSendMessages(
                topicName, numPartitions, numMessages,
                keySchema, valueSchema,
                keyGenerator, valueGenerator);
        });
    }

    private <K, V> void testSendMessages(
        TopicName topicName, int numPartitions, int numMessages,
        Schema<K> keySchema,
        Schema<V> valueSchema,
        Generator<K> keyGenerator,
        Generator<V> valueGenerator
    ) throws Exception {
        MessageRouter kafkaMessageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            true,
            1
        );

        @Cleanup
        MultiVersionKeyValueSchemaProducer producer = new MultiVersionKeyValueSchemaProducer(
            client, topicName.toString(),
            keySchema,
            valueSchema,
            Collections.emptyMap(),
            kafkaMessageRouter,
            schemaManager
        );

        @Cleanup
        Consumer<KeyValue<K, V>> valueConsumer =
            client.newConsumer(Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.SEPARATED))
                .topic(topicName.toString())
                .subscriptionName("value-verification")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // send all the messages
        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>(numPartitions * numMessages);
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                ConsumerRecord<Object, Object> record = newKafkaRecord(
                    topicName.toString(),
                    i,
                    j,
                    keyGenerator,
                    valueGenerator
                );
                sendFutures.add(producer.send(record));
            }
        }
        FutureUtils.collect(sendFutures).get();

        // consume the messages
        Map<Integer, Integer> partitionIdxs = new HashMap<>();
        IntStream.range(0, numPartitions * numMessages).forEach(ignored -> {
            try {
                Message<KeyValue<K, V>> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(topicName.toString(), tn.getPartitionedTopicName());
                int partitionIdx = tn.getPartitionIndex();
                int messageIdxInPtn = partitionIdxs.computeIfAbsent(partitionIdx, p -> -1);

                verifyPulsarMessage(
                    message,
                    partitionIdx,
                    messageIdxInPtn + 1,
                    keyGenerator,
                    valueGenerator
                );

                partitionIdxs.put(partitionIdx, messageIdxInPtn + 1);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Test
    public void testStringKeyMultiVersionedAvroValueSchema() throws Exception {
        int userSchemaId = schemaRegistry.register(
            "user-schema",
            AVRO_USER_SCHEMA
        );
        int studentSchemaId = schemaRegistry.register(
            "student-schema",
            AVRO_STUDENT_SCHEMA
        );

        testSendVersionedMessages(
            10, 10,
            Schema.STRING, new KafkaAvroSchema(),
            STRING_GENERATOR, new MultiVersionGenerator(userSchemaId, studentSchemaId)
        );
    }

    @Test
    public void testMultiVersionedAvroKeyStringValueSchema() throws Exception {
        int userSchemaId = schemaRegistry.register(
            "user-schema",
            AVRO_USER_SCHEMA
        );
        int studentSchemaId = schemaRegistry.register(
            "student-schema",
            AVRO_STUDENT_SCHEMA
        );

        testSendVersionedMessages(
            10, 10,
            new KafkaAvroSchema(), Schema.STRING,
            new MultiVersionGenerator(userSchemaId, studentSchemaId), STRING_GENERATOR
        );
    }

    @Test
    public void testMultiVersionedAvroKeyValueSchema() throws Exception {
        int userSchemaId = schemaRegistry.register(
            "user-schema",
            AVRO_USER_SCHEMA
        );
        int studentSchemaId = schemaRegistry.register(
            "student-schema",
            AVRO_STUDENT_SCHEMA
        );

        testSendVersionedMessages(
            10, 10,
            new KafkaAvroSchema(), new KafkaAvroSchema(),
            new MultiVersionGenerator(userSchemaId, studentSchemaId),
            new MultiVersionGenerator(userSchemaId, studentSchemaId)
        );
    }

    private void testSendVersionedMessages(
        int numPartitions, int numMessages,
        Schema<?> keySchema, Schema<?> valueSchema,
        Generator<?> keyGenerator,
        Generator<?> valueGenerator
    ) throws Exception {

        provisionPartitionedTopic(PUBLIC_TENANT, numPartitions, topicName -> {
            testSendVersionedMessages(
                topicName, numPartitions, numMessages,
                keySchema, valueSchema,
                keyGenerator, valueGenerator);
        });
    }

    private void testSendVersionedMessages(
        TopicName topicName, int numPartitions, int numMessages,
        Schema<?> keySchema,
        Schema<?> valueSchema,
        Generator<?> keyGenerator,
        Generator<?> valueGenerator
    ) throws Exception {
        MessageRouter kafkaMessageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            true,
            1
        );

        @Cleanup
        MultiVersionKeyValueSchemaProducer producer = new MultiVersionKeyValueSchemaProducer(
            client, topicName.toString(),
            keySchema,
            valueSchema,
            Collections.emptyMap(),
            kafkaMessageRouter,
            schemaManager
        );

        // send all the messages
        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>(numPartitions * numMessages);
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                ConsumerRecord<Object, Object> record = newKafkaRecord(
                    topicName.toString(),
                    i,
                    j,
                    keyGenerator,
                    valueGenerator
                );
                sendFutures.add(producer.send(record));
            }
        }
        FutureUtils.collect(sendFutures).get();

        @Cleanup
        Consumer<byte[]> valueConsumer =
            client.newConsumer(Schema.BYTES)
                .topic(topicName.toString())
                .subscriptionName("value-verification")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe();

        // consume the messages
        Map<KeyValue<Integer, Integer>, Message<byte[]>> messages = new HashMap<>();
        IntStream.range(0, numPartitions * numMessages).forEach(ignored -> {
            try {
                Message<byte[]> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(topicName.toString(), tn.getPartitionedTopicName());
                int partitionIdx = tn.getPartitionIndex();
                int sequence = (int) (message.getSequenceId() / 1000L);
                KeyValue<Integer, Integer> kv = new KeyValue<>(partitionIdx, sequence);
                assertFalse(messages.containsKey(kv));
                messages.put(kv, message);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });

        assertEquals(numPartitions * numMessages, messages.size());

        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                KeyValue<Integer, Integer> kv = new KeyValue<>(i, j);
                Message<byte[]> message = messages.get(kv);
                assertEquals(j * 1000L, message.getSequenceId());
                assertEquals((j + 1) * 10000L, message.getEventTime());

                verifyMultiVersionData(
                    i, j,
                    message.getKeyBytes(),
                    keySchema,
                    keyGenerator
                );

                verifyMultiVersionData(
                    i, j,
                    message.getValue(),
                    valueSchema,
                    valueGenerator
                );
            }
        }
    }

}
