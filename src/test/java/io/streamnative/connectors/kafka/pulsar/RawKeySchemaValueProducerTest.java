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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A system test to test producer with raw key and schema value.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(PulsarProducerTestSuite.class)
@Slf4j
public class RawKeySchemaValueProducerTest extends PulsarProducerTestBase {

    public RawKeySchemaValueProducerTest(PulsarService service) {
        super(service);
    }

    private <V> void verifyPulsarMessage(Message<V> pulsarMessage,
                                         int partition,
                                         int i,
                                         Generator<V> valueGenerator,
                                         boolean nullKey) {
        assertEquals(i * 1000L, pulsarMessage.getSequenceId());
        assertEquals((i + 1) * 10000L, pulsarMessage.getEventTime());
        if (nullKey) {
            assertFalse(pulsarMessage.hasKey());
        } else {
            assertArrayEquals(
                KEY_GENERATOR.apply(partition, i),
                pulsarMessage.getKeyBytes()
            );
        }
        if (valueGenerator == null) {
            Object value = pulsarMessage.getValue();
            if (value instanceof byte[]) {
                assertTrue(((byte[]) value).length == 0);
            } else {
                assertNull(value);
            }
        } else {
            if (pulsarMessage.getValue() instanceof byte[]) {
                assertArrayEquals(
                    (byte[]) valueGenerator.apply(partition, i),
                    (byte[]) pulsarMessage.getValue()
                );
            } else {
                assertEquals(
                    valueGenerator.apply(partition, i),
                    pulsarMessage.getValue()
                );
            }
        }
    }

    @Test
    public void testBytesSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.BYTES,
            BYTES_GENERATOR);
    }

    @Test
    public void testBooleanSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.BOOL,
            BOOLEAN_GENERATOR);
    }

    @Test
    public void testInt8Schema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT8,
            BYTE_GENERATOR);
    }

    @Test
    public void testInt16Schema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT16,
            SHORT_GENERATOR);
    }

    @Test
    public void testInt32chema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT32,
            INTEGER_GENERATOR);
    }

    @Test
    public void testInt64chema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT64,
            LONG_GENERATOR);
    }

    @Test
    public void testFloatchema() throws Exception {
        testSendMessages(
            10, 10, Schema.FLOAT,
            FLOAT_GENERATOR);
    }

    @Test
    public void testDoublechema() throws Exception {
        testSendMessages(
            10, 10, Schema.DOUBLE,
            DOUBLE_GENERATOR);
    }

    @Test
    public void testStringSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.STRING,
            STRING_GENERATOR);
    }

    @Test
    public void testAvroSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.AVRO(
                SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .withAlwaysAllowNull(false)
                    .build()
            ),
            USER_GENERATOR);
    }

    @Test
    public void testJsonSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.JSON(
                SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .build()
            ),
            USER_GENERATOR);
    }

    @Test
    public void testNullKeyAvroSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.AVRO(
                SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .withAlwaysAllowNull(false)
                    .build()
            ),
            USER_GENERATOR,
            true);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4803")
    public void testNullValueAllSchemas() throws Exception {
        Schema<?>[] schemas = new Schema<?>[] {
            Schema.BYTES,
            Schema.BOOL,
            Schema.INT8,
            Schema.INT16,
            Schema.INT32,
            Schema.INT64,
            Schema.FLOAT,
            Schema.DOUBLE,
            Schema.STRING,
            Schema.AVRO(
                SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .withAlwaysAllowNull(false)
                    .build()
            ),
            Schema.JSON(
                SchemaDefinition.<User>builder()
                    .withPojo(User.class)
                    .withAlwaysAllowNull(false)
                    .build()
            ),
        };

        for (Schema<?> schema : schemas) {
            try {
                testNullValue(schema);
            } catch (AssertionError e) {
                throw new AssertionError("Exception thrown when verifying schema "
                    + schema.getSchemaInfo().getType(), e);
            }
        }
    }

    private void testNullValue(Schema<?> schema) throws Exception {
        testSendMessages(
            10, 10,
            schema,
            null,
            false
        );
    }

    private <V> void testSendMessages(int numPartitions, int numMessages,
                                      Schema<V> valueSchema,
                                      Generator<V> valueGenerator) throws Exception {
        testSendMessages(numPartitions, numMessages, valueSchema, valueGenerator, false);
    }

    private <V> void testSendMessages(int numPartitions, int numMessages,
                                      Schema<V> valueSchema,
                                      Generator<V> valueGenerator,
                                      boolean nullKey) throws Exception {
        provisionPartitionedTopic(PUBLIC_TENANT, numPartitions,
            topicName -> testSendMessages(topicName, numPartitions, numMessages, valueSchema, valueGenerator, nullKey));
    }

    private <V> void testSendMessages(TopicName topicName, int numPartitions, int numMessages,
                                      Schema<V> valueSchema,
                                      Generator<V> valueGenerator,
                                      boolean nullKey) throws Exception {
        MessageRouter kafkaMessageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            true,
            1
        );
        @Cleanup
        RawKeySchemaValueProducer producer = new RawKeySchemaValueProducer(
            client, topicName.toString(), valueSchema,
            Collections.emptyMap(),
            kafkaMessageRouter
        );

        @Cleanup
        Consumer<V> valueConsumer = client.newConsumer(valueSchema)
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
                    nullKey ? null : KEY_GENERATOR,
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
                Message<V> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(topicName.toString(), tn.getPartitionedTopicName());
                int partitionIdx = tn.getPartitionIndex();
                int messageIdxInPtn = partitionIdxs.computeIfAbsent(partitionIdx, p -> -1);

                verifyPulsarMessage(
                    message,
                    partitionIdx,
                    messageIdxInPtn + 1,
                    valueGenerator,
                    nullKey
                );

                partitionIdxs.put(partitionIdx, messageIdxInPtn + 1);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
