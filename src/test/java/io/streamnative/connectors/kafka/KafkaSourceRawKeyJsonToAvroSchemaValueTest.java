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

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_STUDENT_SCHEMA_DEF;
import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.PULSAR_STUDENT_SCHEMA;

import io.streamnative.connectors.kafka.KafkaSourceConfig.JsonSchemaProvider;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.Student;
import io.streamnative.connectors.kafka.schema.KafkaAvroSchemaManagerConfig;
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
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.utility.Base58;

/**
 * Integration test for {@link KafkaSource}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(KafkaSourceTestSuite.class)
@Slf4j
@SuppressWarnings("unchecked")
public class KafkaSourceRawKeyJsonToAvroSchemaValueTest extends KafkaSourceAvroSchemaTestBase {

    public KafkaSourceRawKeyJsonToAvroSchemaValueTest(PulsarService service) {
        super(service);
    }

    @Test
    public void testPulsarSchemaProvider() throws Exception {
        final int numPartitions = 10;
        final int numMessages = 10;
        provisionPartitionedTopic(TopicName.PUBLIC_TENANT, numPartitions, topicName ->
            testJsonToAvroSchema(
                JsonSchemaProvider.PULSAR,
                topicName.toString(),
                numPartitions,
                numMessages
        ));
    }

    @Test
    public void testConfigSchemaProvider() throws Exception {
        final int numPartitions = 10;
        final int numMessages = 10;
        testJsonToAvroSchema(
            JsonSchemaProvider.CONFIG,
            "test-config-schema-provider-" + Base58.randomString(8),
            numPartitions,
            numMessages
        );
    }

    private void testJsonToAvroSchema(final JsonSchemaProvider schemaProvider,
                                     final String pulsarTopic,
                                     final int numPartitions,
                                     final int numMessages) throws Exception {
        String kafkaTopic = TopicName.get(pulsarTopic).getLocalName();
        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(kafkaTopic);
        config.kafka().consumer()
            .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.kafka().consumer()
            .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
        config.kafka().consumer()
            .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());
        Map<String, Object> schemaRegistryConfigMap = new HashMap<>();
        schemaRegistryConfigMap.put(
            KafkaAvroSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryServiceUri.getUri().toString()
        );
        config.kafka().schema().schema_registry(schemaRegistryConfigMap);
        config.kafka().schema().json_schema_provider(schemaProvider);
        config.pulsar().copy_kafka_schema(true);
        config.pulsar().topic(pulsarTopic);

        createKafkaTopic(kafkaTopic, numPartitions);

        if (JsonSchemaProvider.PULSAR == schemaProvider) {
            // upload schema to ensure schema registered first
            admin.schemas().createSchema(
                pulsarTopic, PULSAR_STUDENT_SCHEMA.getSchemaInfo());
        } else {
            config.kafka().schema().value_schema(
                AVRO_STUDENT_SCHEMA_DEF
            );
        }

        KafkaSource source = new KafkaSource();
        try {
            source.open(config.toConfigMap(), ctx);

            testKafkaSourceSendAndReceiveJsonValues(
                kafkaTopic, pulsarTopic, numPartitions, numMessages
            );
        } finally {
            source.close();
        }
    }

    protected void testKafkaSourceSendAndReceiveJsonValues(
        String kafkaTopic, String pulsarTopic,
        int numPartitions, int numMessages
    ) throws Exception {
        @Cleanup
        Consumer<Student> studentConsumer = client.newConsumer(PULSAR_STUDENT_SCHEMA)
            .topic(pulsarTopic)
            .subscriptionName("test-verifier")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        sendJsonMessagesToKafka(kafkaTopic, numPartitions, numMessages);

        receiveJsonKeyAvroValuesFromPulsar(
            studentConsumer,
            pulsarTopic,
            numPartitions, numMessages
        );
    }
}
