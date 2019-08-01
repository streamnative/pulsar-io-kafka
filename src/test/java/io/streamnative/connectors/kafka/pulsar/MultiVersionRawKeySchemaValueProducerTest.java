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
import io.streamnative.connectors.kafka.KafkaAvroSchema;
import io.streamnative.connectors.kafka.KafkaMessageRouter;
import io.streamnative.connectors.kafka.KafkaSchemaManager;
import io.streamnative.connectors.kafka.KafkaSchemaManagerConfig;
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
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A system test to test producer with raw key and schema value.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(PulsarProducerTestSuite.class)
@Slf4j
public class MultiVersionRawKeySchemaValueProducerTest extends PulsarProducerTestBase {

    private final SchemaRegistryClient schemaRegistry;
    private final KafkaSchemaManager schemaManager;

    public MultiVersionRawKeySchemaValueProducerTest(PulsarService service) {
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

    @Test
    public void testMultiVersionedAvroValueSchema() throws Exception {
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
            new KafkaAvroSchema(),
            new MultiVersionGenerator(userSchemaId, studentSchemaId)
        );
    }

    private void testSendVersionedMessages(
        int numPartitions, int numMessages,
        Schema<?> valueSchema,
        Generator<?> valueGenerator
    ) throws Exception {
        provisionPartitionedTopic(PUBLIC_TENANT, numPartitions, topicName -> {
            testSendVersionedMessages(
                topicName, numPartitions, numMessages,
                valueSchema,
                valueGenerator);
        });
    }

    private void testSendVersionedMessages(
        TopicName topicName, int numPartitions, int numMessages,
        Schema<?> valueSchema,
        Generator<?> valueGenerator
    ) throws Exception {
        MessageRouter kafkaMessageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            true,
            1
        );

        @Cleanup
        MultiVersionRawKeySchemaValueProducer producer = new MultiVersionRawKeySchemaValueProducer(
            client, topicName.toString(),
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
                    BYTES_GENERATOR,
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
                    Schema.BYTES,
                    BYTES_GENERATOR
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
