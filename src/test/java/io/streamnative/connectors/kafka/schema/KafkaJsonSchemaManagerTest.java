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
package io.streamnative.connectors.kafka.schema;

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_STUDENT_SCHEMA;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_USER_SCHEMA;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.PULSAR_STUDENT_SCHEMA;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.PULSAR_USER_SCHEMA;
import static io.streamnative.connectors.kafka.schema.KafkaJsonSchemaManager.getConverter;
import static io.streamnative.connectors.kafka.schema.KafkaJsonSchemaManager.getSchemaInfo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.confluent.connect.avro.AvroConverter;
import io.streamnative.connectors.kafka.KafkaSourceConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.ConverterType;
import io.streamnative.connectors.kafka.KafkaSourceConfig.JsonSchemaProvider;
import io.streamnative.connectors.kafka.KafkaSourceConfig.KafkaConsumerConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.PulsarProducerConfig;
import io.streamnative.connectors.kafka.KafkaSourceTestSuite;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.Student;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.User;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import org.apache.avro.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.utility.Base58;

/**
 * Unit test {@link KafkaJsonSchemaManager}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(KafkaSourceTestSuite.class)
public class KafkaJsonSchemaManagerTest extends PulsarServiceSystemTestCase {

    public KafkaJsonSchemaManagerTest(PulsarService service) {
        super(service);
    }

    @Test
    public void testGetConverter() {
        assertTrue(getConverter(null, true) instanceof AvroConverter);
        assertTrue(getConverter(ConverterType.AVRO, true) instanceof AvroConverter);
        assertTrue(getConverter(ConverterType.JSON, true) instanceof JsonConverter);
    }

    @Test
    public void testGetSchemaInfo() {
        KeyValue<Schema, Converter> kv = getSchemaInfo(
            PULSAR_USER_SCHEMA.getSchemaInfo(),
            true
        );
        assertEquals(
            PulsarProducerTestBase.AVRO_USER_SCHEMA,
            kv.getKey()
        );
        assertTrue(kv.getValue() instanceof AvroConverter);

        kv = getSchemaInfo(
            PULSAR_USER_SCHEMA.getSchemaInfo(),
            true
        );
        assertEquals(
            PulsarProducerTestBase.AVRO_USER_SCHEMA,
            kv.getKey()
        );
        assertTrue(kv.getValue() instanceof AvroConverter);

        kv = getSchemaInfo(org.apache.pulsar.client.api.Schema.BYTES.getSchemaInfo(), true);
        assertNull(kv.getKey());
        assertNull(kv.getValue());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConfigSchemaProviderNoSchemaConfigured() throws Exception {
        KafkaSourceConfig config = new KafkaSourceConfig();
        config.kafka(new KafkaConsumerConfig());
        config.kafka().schema().json_schema_provider(JsonSchemaProvider.CONFIG);

        new KafkaJsonSchemaManager(admin, config);
        fail("Should not reach here");
    }

    @Test
    public void testConfigSchemaProviderDefaultConverter() throws Exception {
        KafkaSourceConfig config = new KafkaSourceConfig();
        config.kafka(new KafkaConsumerConfig());
        config.kafka().schema().json_schema_provider(JsonSchemaProvider.CONFIG);
        config.kafka().schema().key_schema(AVRO_STUDENT_SCHEMA.toString());
        config.kafka().schema().value_schema(AVRO_USER_SCHEMA.toString());

        KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager(
            admin, config
        );
        assertEquals(
            AVRO_STUDENT_SCHEMA, manager.keySchema()
        );
        assertEquals(
            AVRO_USER_SCHEMA, manager.valueSchema()
        );
        assertTrue(manager.keyConverter() instanceof AvroConverter);
        assertTrue(manager.valueConverter() instanceof AvroConverter);
    }

    @Test
    public void testConfigSchemaProviderJsonConverter() throws Exception {
        KafkaSourceConfig config = new KafkaSourceConfig();
        config.kafka(new KafkaConsumerConfig());
        config.kafka().schema().json_schema_provider(JsonSchemaProvider.CONFIG);
        config.kafka().schema().key_schema(AVRO_STUDENT_SCHEMA.toString());
        config.kafka().schema().key_converter(ConverterType.JSON);
        config.kafka().schema().value_schema(AVRO_USER_SCHEMA.toString());
        config.kafka().schema().value_converter(ConverterType.JSON);

        KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager(
            admin, config
        );
        assertEquals(
            AVRO_STUDENT_SCHEMA, manager.keySchema()
        );
        assertEquals(
            AVRO_USER_SCHEMA, manager.valueSchema()
        );
        assertTrue(manager.keyConverter() instanceof JsonConverter);
        assertTrue(manager.valueConverter() instanceof JsonConverter);
    }

    @Test
    public void testPulsarSchemaProviderAvroSchema() throws Exception {
        final String topic = "test-pulsar-schema-provider-avro-schema-" + Base58.randomString(8);

        KafkaSourceConfig config = new KafkaSourceConfig();
        config.kafka(new KafkaConsumerConfig());
        config.kafka().schema().json_schema_provider(JsonSchemaProvider.PULSAR);
        config.pulsar(new PulsarProducerConfig());
        config.pulsar().topic(topic);

        admin.schemas().createSchema(topic, PULSAR_USER_SCHEMA.getSchemaInfo());

        KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager(
            admin, config
        );
        assertNull(manager.keySchema());
        assertTrue(manager.keyConverter() instanceof AvroConverter);
        assertEquals(AVRO_USER_SCHEMA, manager.valueSchema());
        assertTrue(manager.valueConverter() instanceof AvroConverter);
    }

    @Test
    public void testPulsarSchemaProviderJsonSchema() throws Exception {
        final String topic = "test-pulsar-schema-provider-json-schema-" + Base58.randomString(8);

        KafkaSourceConfig config = new KafkaSourceConfig();
        config.kafka(new KafkaConsumerConfig());
        config.kafka().schema().json_schema_provider(JsonSchemaProvider.PULSAR);
        config.pulsar(new PulsarProducerConfig());
        config.pulsar().topic(topic);

        org.apache.pulsar.client.api.Schema<User> jsonSchema =
            org.apache.pulsar.client.api.Schema.JSON(User.class);
        admin.schemas().createSchema(topic, jsonSchema.getSchemaInfo());

        KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager(
            admin, config
        );
        assertNull(manager.keySchema());
        assertTrue(manager.keyConverter() instanceof JsonConverter);
        assertEquals(
            new Schema.Parser().parse(jsonSchema.getSchemaInfo().getSchemaDefinition()),
            manager.valueSchema());
        assertTrue(manager.valueConverter() instanceof JsonConverter);
    }

    @Ignore(value = "https://github.com/apache/pulsar/issues/4840")
    public void testPulsarSchemaProviderKeyValueSchema() throws Exception {
        provisionPartitionedTopic(TopicName.PUBLIC_TENANT, 4, topicName -> {

            KafkaSourceConfig config = new KafkaSourceConfig();
            config.kafka(new KafkaConsumerConfig());
            config.kafka().schema().json_schema_provider(JsonSchemaProvider.PULSAR);
            config.pulsar(new PulsarProducerConfig());
            config.pulsar().topic(topicName.toString());

            org.apache.pulsar.client.api.Schema<User> keySchema =
                    org.apache.pulsar.client.api.Schema.JSON(User.class);
            org.apache.pulsar.client.api.Schema<KeyValue<User, Student>> kvSchema =
                org.apache.pulsar.client.api.Schema.KeyValue(
                    keySchema,
                    PULSAR_STUDENT_SCHEMA);
            try (Consumer<KeyValue<User, Student>> consumer = client.newConsumer(kvSchema)
                 .topic(topicName.toString())
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscriptionName("test-sub")
                 .subscribe()) {
                // no-op
            }

            KafkaJsonSchemaManager manager = new KafkaJsonSchemaManager(
                admin, config
            );
            assertEquals(
                new Schema.Parser().parse(
                    kvSchema.getSchemaInfo().getSchemaDefinition()),
                manager.keySchema());
            assertTrue(manager.keyConverter() instanceof JsonConverter);
            assertEquals(
                AVRO_STUDENT_SCHEMA,
                manager.valueSchema());
            assertTrue(manager.valueConverter() instanceof AvroConverter);

        });
    }

}
