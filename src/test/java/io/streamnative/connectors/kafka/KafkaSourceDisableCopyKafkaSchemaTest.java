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

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.BYTES_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.DOUBLE_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.FLOAT_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.INTEGER_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.LONG_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.SHORT_GENERATOR;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.STRING_GENERATOR;

import io.streamnative.connectors.kafka.pulsar.Generator;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.FloatSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pulsar.client.api.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.utility.Base58;

/**
 * Integration test for {@link KafkaSource}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(KafkaSourceTestSuite.class)
@Slf4j
public class KafkaSourceDisableCopyKafkaSchemaTest extends KafkaSourceTestBase {

    public KafkaSourceDisableCopyKafkaSchemaTest(PulsarService service) {
        super(service);
    }

    @Test
    public void testBytesSchemaOneSource() throws Exception {
        testBytesSchema(1);
    }

    @Test
    public void testBytesSchemaFiveSources() throws Exception {
        testBytesSchema(5);
    }

    private void testBytesSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new ByteArraySerializer(),
                BYTES_GENERATOR,
                Schema.BYTES
            ),
            numSources
        );
    }

    @Test
    public void testStringSchemaOneSource() throws Exception {
        testStringSchema(1);
    }

    @Test
    public void testStringSchemaFiveSources() throws Exception {
        testStringSchema(5);
    }

    private void testStringSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new StringSerializer(),
                STRING_GENERATOR,
                Schema.STRING
            ),
            numSources
        );
    }

    @Test
    public void testDoubleSchemaOneSource() throws Exception {
        testDoubleSchema(1);
    }

    @Test
    public void testDoubleSchemaFiveSources() throws Exception {
        testDoubleSchema(5);
    }

    private void testDoubleSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new DoubleSerializer(),
                DOUBLE_GENERATOR,
                Schema.DOUBLE
            ),
            numSources
        );
    }

    @Test
    public void testFloatSchemaOneSource() throws Exception {
        testFloatSchema(1);
    }

    @Test
    public void testFloatSchemaFiveSources() throws Exception {
        testFloatSchema(5);
    }

    private void testFloatSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new FloatSerializer(),
                FLOAT_GENERATOR,
                Schema.FLOAT
            ),
            numSources
        );
    }

    @Test
    public void testIntegerSchemaOneSource() throws Exception {
        testIntegerSchema(1);
    }

    @Test
    public void testIntegerSchemaFiveSources() throws Exception {
        testIntegerSchema(5);
    }

    private void testIntegerSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new IntegerSerializer(),
                INTEGER_GENERATOR,
                Schema.INT32
            ),
            numSources
        );
    }

    @Test
    public void testLongSchemaOneSource() throws Exception {
        testLongSchema(1);
    }

    @Test
    public void testLongSchemaFiveSources() throws Exception {
        testLongSchema(5);
    }

    private void testLongSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new LongSerializer(),
                LONG_GENERATOR,
                Schema.INT64
            ),
            numSources
        );
    }

    @Test
    public void testShortSchemaOneSource() throws Exception {
        testShortSchema(1);
    }

    @Test
    public void testShortSchemaFiveSources() throws Exception {
        testShortSchema(5);
    }

    private void testShortSchema(int numSources) throws Exception {
        testDisableCopyKafkaSchema(
            new DisableCopyKafkaSchemaTester<>(
                new ShortSerializer(),
                SHORT_GENERATOR,
                Schema.INT16
            ),
            numSources
        );
    }

    @SuppressWarnings("unchecked")
    public <T> void testDisableCopyKafkaSchema(DisableCopyKafkaSchemaTester<T> tester,
                                               int numSources) throws Exception {
        testDisableCopyKafkaSchema(
                    tester.serializer,
                    tester.generator,
                    disabledSchema(tester.pulsarSchema),
                    numSources
                );
    }

    @RequiredArgsConstructor
    static class DisableCopyKafkaSchemaTester<T> {

        final Serializer<T> serializer;
        final Generator<T> generator;
        final Schema<T> pulsarSchema;

    }

    private <T> void testDisableCopyKafkaSchema(
        Serializer<T> serializer, Generator<T> generator,
        Schema<T> pulsarSchema, int numSources
    ) throws Exception {
        String topic = "test-disable-copy-kafka-schema-"
            + serializer.getClass().getSimpleName() + "-"
            + numSources + "-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.kafka().consumer()
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, serializer.getClass().getName());
        config.kafka().consumer()
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, serializer.getClass().getName());
        config.kafka().consumer()
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        config.pulsar().copy_kafka_schema(false);

        final int numPartitions = 10;
        final int numMessages = 10;
        createKafkaTopic(topic, numPartitions);
        admin.topics().createPartitionedTopic(topic, numPartitions);

        KafkaSource[] sources = new KafkaSource[numSources];
        for (int i = 0; i < numSources; i++) {
            sources[i] = new KafkaSource();
        }

        try {
            for (KafkaSource source : sources) {
                source.open(config.toConfigMap(), ctx);
            }

            testKafkaSourceSendAndReceiveMessagesRawKey(
                topic, topic,
                numPartitions, numMessages,
                serializer, serializer,
                generator, generator,
                pulsarSchema
            );
        } finally {
            for (KafkaSource source : sources) {
                source.close();
            }
        }
    }

}
