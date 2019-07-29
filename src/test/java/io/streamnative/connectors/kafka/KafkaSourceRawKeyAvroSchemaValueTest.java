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

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_VALUE_VERIFIER;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.PULSAR_STUDENT_SCHEMA;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.PULSAR_USER_SCHEMA;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import java.util.HashMap;
import java.util.Map;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.utility.Base58;

/**
 * Integration test for {@link KafkaSource}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(KafkaSourceTestSuite.class)
@Slf4j
public class KafkaSourceRawKeyAvroSchemaValueTest extends KafkaSourceAvroSchemaTestBase {

    public KafkaSourceRawKeyAvroSchemaValueTest(PulsarService service) {
        super(service);
    }

    @Test
    public void testMultiVersionedAvroValueSchema() throws Exception {
        String topic = "test-multi-versioned-avro-value-schema-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.kafka().consumer()
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.kafka().consumer()
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        config.kafka().consumer()
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        Map<String, Object> schemaRegistryConfigMap = new HashMap<>();
        schemaRegistryConfigMap.put(
            KafkaSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryServiceUri.getUri().toString()
        );
        config.kafka().schema().schema_registry(schemaRegistryConfigMap);
        config.pulsar().copy_kafka_schema(true);

        final int numPartitions = 10;
        final int numMessages = 10;
        createKafkaTopic(topic, numPartitions);
        admin.topics().createPartitionedTopic(topic, numPartitions);

        // upload schema to ensure schema registered first
        admin.schemas().createSchema(
            topic, PULSAR_USER_SCHEMA.getSchemaInfo());
        admin.schemas().createSchema(
            topic, PULSAR_STUDENT_SCHEMA.getSchemaInfo());

        KafkaSource source = new KafkaSource();
        try {
            source.open(config.toConfigMap(), ctx);

            testKafkaSourceSendAndReceiveAvroValues(
                topic, topic, numPartitions, numMessages
            );
        } finally {
            source.close();
        }
    }

    protected void testKafkaSourceSendAndReceiveAvroValues(
        String kafkaTopic, String pulsarTopic,
        int numPartitions, int numMessages
    ) throws Exception {
        @Cleanup
        Consumer<GenericRecord> studentConsumer = client.newConsumer(Schema.AUTO_CONSUME())
            .topic(pulsarTopic)
            .subscriptionName("test-verifier")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        sendAvroMessagesToKafka(kafkaTopic, numPartitions, numMessages);

        receiveAvroValuesFromPulsar(
            studentConsumer,
            pulsarTopic,
            numPartitions, numMessages,
            AVRO_VALUE_VERIFIER
        );
    }
}
