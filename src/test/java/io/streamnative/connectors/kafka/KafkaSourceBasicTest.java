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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.streamnative.connectors.kafka.KafkaSourceConfig.KafkaConsumerConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.PulsarProducerConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Test;

/**
 * Test Kafka Source without copying schema.
 */
@Slf4j
public class KafkaSourceBasicTest {

    private final SourceContext ctx;

    public KafkaSourceBasicTest() {
        this.ctx = mock(SourceContext.class);
    }

    @Test
    public void testOpenWithoutKafkaSettings() throws Exception {
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig();
        Map<String, Object> mapConfig = sourceConfig.toConfigMap();

        try {
            new KafkaSource().open(mapConfig, ctx);
            fail("Should not open a source with a bad configuration");
        } catch (NullPointerException npe) {
            assertEquals(
                "The Kafka settings are missing",
                npe.getMessage());
        }
    }

    @Test
    public void testOpenKafkaSettingsWithoutTopic() throws Exception {
        testOpenWithInvalidKafkaSettings(new KafkaConsumerConfig());
    }

    @Test
    public void testOpenKafkaSettingsWithoutConsumerSettings() throws Exception {
        testOpenWithInvalidKafkaSettings(new KafkaConsumerConfig()
            .topic("test-topic"));
    }

    public void testOpenWithInvalidKafkaSettings(KafkaConsumerConfig consumerConfig) throws Exception {
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig()
            .kafka(consumerConfig);
        Map<String, Object> mapConfig = sourceConfig.toConfigMap();

        try {
            new KafkaSource().open(mapConfig, ctx);
            fail("Should not open a source with an invalid kafka configuration");
        } catch (NullPointerException npe) {
            // expected
        }
    }

    @Test
    public void testOpenWithoutPulsarSettings() throws Exception {
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig()
            .kafka(new KafkaConsumerConfig()
                .topic("test-topic")
                .consumer(Collections.emptyMap()));
        Map<String, Object> mapConfig = sourceConfig.toConfigMap();

        try {
            new KafkaSource().open(mapConfig, ctx);
            fail("Should not open a source with a bad configuration");
        } catch (NullPointerException npe) {
            assertEquals(
                "The Pulsar settings are missing",
                npe.getMessage());
        }
    }

    @Test
    public void testOpenPulsarSettingsWithoutClientSettings() throws Exception {
        testOpenWithInvalidPulsarSettings(new PulsarProducerConfig());
    }

    public void testOpenWithInvalidPulsarSettings(PulsarProducerConfig producerConfig) throws Exception {
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig()
            .kafka(new KafkaConsumerConfig()
                .topic("test-topic")
                .consumer(Collections.emptyMap()))
            .pulsar(producerConfig);
        Map<String, Object> mapConfig = sourceConfig.toConfigMap();

        try {
            new KafkaSource().open(mapConfig, ctx);
            fail("Should not open a source with an invalid kafka configuration");
        } catch (NullPointerException npe) {
            // expected
        }
    }

    @Test
    public void testGetKafkaDeserializerClassKeyNotSet() throws Exception {
        Map<String, Object> config = Collections.emptyMap();
        try {
            KafkaSource.getKafkaDeserializerClass(config, "not-set-key");
            fail("Should throw exception if key is not set");
        } catch (NullPointerException npe) {
            assertTrue(npe.getMessage().contains("is not set"));
        }
    }

    @Test
    public void testGetKafkaDeserializerClassKeyInvalidValue() throws Exception {
        Map<String, Object> config = new HashMap<>();
        final String key = "invalid-value-key";
        config.put(key, new Object());
        try {
            KafkaSource.getKafkaDeserializerClass(config, key);
            fail("Should throw exception if value is invalid");
        } catch (IllegalArgumentException npe) {
            assertTrue(npe.getMessage().contains("Unknown deserializer class"));
        }
    }

    @Test
    public void testGetPulsarKeySchemaAndCopyKafkaSchemaGoodCase() {
        testGetPulsarSchemaCopyKafkaSchemaGoodCase(true);
    }

    @Test
    public void testGetPulsarValueSchemaCopyKafkaSchemaGoodCase() {
        testGetPulsarSchemaCopyKafkaSchemaGoodCase(false);
    }

    @SuppressWarnings("unchecked")
    private void testGetPulsarSchemaCopyKafkaSchemaGoodCase(boolean isKey) {
        Deserializer<?> mockDeserializer = mock(Deserializer.class);
        Class<? extends Deserializer<?>> mockDeserializerClass =
            (Class<? extends Deserializer<?>>) mockDeserializer.getClass();
        Map<Class<? extends Deserializer<?>>, SchemaType> testPairs = new HashMap<>();
        testPairs.put(ByteArrayDeserializer.class, SchemaType.BYTES);
        testPairs.put(ByteBufferDeserializer.class, SchemaType.BYTES);
        testPairs.put(BytesDeserializer.class, SchemaType.BYTES);
        testPairs.put(StringDeserializer.class, SchemaType.STRING);
        testPairs.put(DoubleDeserializer.class, SchemaType.DOUBLE);
        testPairs.put(FloatDeserializer.class, SchemaType.FLOAT);
        testPairs.put(IntegerDeserializer.class, SchemaType.INT32);
        testPairs.put(LongDeserializer.class, SchemaType.INT64);
        testPairs.put(ShortDeserializer.class, SchemaType.INT16);
        testPairs.put(KafkaAvroDeserializer.class, null);
        testPairs.put(mockDeserializerClass, null);

        testGetPulsarKeySchemaAndCopyKafkaSchemaGoodCase(
            testPairs, isKey, true);
    }

    @Test
    public void testGetPulsarKeySchemaNotCopyKafkaSchemaGoodCase() {
        testGetPulsarSchemaNotCopyKafkaSchemaGoodCase(true);
    }

    @Test
    public void testGetPulsarValueSchemaNotCopyKafkaSchemaGoodCase() {
        testGetPulsarSchemaNotCopyKafkaSchemaGoodCase(false);
    }

    @SuppressWarnings("unchecked")
    private void testGetPulsarSchemaNotCopyKafkaSchemaGoodCase(boolean isKey) {
        Deserializer<?> mockDeserializer = mock(Deserializer.class);
        Class<? extends Deserializer<?>> mockDeserializerClass =
            (Class<? extends Deserializer<?>>) mockDeserializer.getClass();
        Map<Class<? extends Deserializer<?>>, SchemaType> testPairs = new HashMap<>();
        testPairs.put(ByteArrayDeserializer.class, null);
        testPairs.put(ByteBufferDeserializer.class, null);
        testPairs.put(BytesDeserializer.class, null);
        testPairs.put(StringDeserializer.class, null);
        testPairs.put(DoubleDeserializer.class, null);
        testPairs.put(FloatDeserializer.class, null);
        testPairs.put(IntegerDeserializer.class, null);
        testPairs.put(LongDeserializer.class, null);
        testPairs.put(ShortDeserializer.class, null);
        testPairs.put(KafkaAvroDeserializer.class, null);
        testPairs.put(mockDeserializerClass, null);

        testGetPulsarKeySchemaAndCopyKafkaSchemaGoodCase(
            testPairs, isKey, false);
    }

    private void testGetPulsarKeySchemaAndCopyKafkaSchemaGoodCase(
        Map<Class<? extends Deserializer<?>>, SchemaType> testPairs,
        boolean isKey, boolean copyKafkaSchema
    ) {
        testPairs.forEach((k, v) -> {
            try {
                testGetPulsarSchemaGoodCase(k, v, isKey, copyKafkaSchema);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } catch (AssertionError ae) {
                throw new AssertionError(
                    "Failed to process test case (" + k + ", " + v + ")",
                    ae
                );
            }
        });
    }

    private void testGetPulsarSchemaGoodCase(Class<? extends Deserializer<?>> kafkaDeserializerClass,
                                             SchemaType pulsarSchemaType,
                                             boolean isKey,
                                             boolean copyKafkaSchema) throws Exception {
        String deserializerClassKey;
        if (isKey) {
            deserializerClassKey = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
        } else {
            deserializerClassKey = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
        }
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(deserializerClassKey, kafkaDeserializerClass.getName());
        KafkaSourceConfig sourceConfig = new KafkaSourceConfig()
            .kafka(
                new KafkaConsumerConfig()
                    .consumer(consumerConfig)
            )
            .pulsar(
                new PulsarProducerConfig()
                    .copy_kafka_schema(copyKafkaSchema)
            );

        Schema schema = KafkaSource.getPulsarSchemaAndReconfigureDeserializerClass(
            sourceConfig,
            isKey
        );
        if (null == schema) {
            assertNull(pulsarSchemaType);
            // the deserializer class value will be set to ByteArrayDeserializer
            assertEquals(
                ByteArrayDeserializer.class.getName(),
                consumerConfig.get(deserializerClassKey)
            );
        } else {
            assertEquals(pulsarSchemaType, schema.getSchemaInfo().getType());
        }
    }

}
