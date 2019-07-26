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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.streamnative.connectors.kafka.KafkaSourceConfig.KafkaConsumerConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.PulsarProducerConfig;
import io.streamnative.connectors.kafka.pulsar.Generator;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.PulsarAdminException.NotFoundException;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.io.core.SourceContext;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.utility.Base58;

/**
 * Integration test for {@link KafkaSource}.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(KafkaSourceTestSuite.class)
@Slf4j
public class KafkaSourceTest extends KafkaSourceTestBase {

    private final SourceContext ctx;
    private AdminClient kafkaAdmin;

    public KafkaSourceTest(PulsarService service) {
        super(service);
        this.ctx = mock(SourceContext.class);
    }

    @Override
    protected void doSetup() throws Exception {
        super.doSetup();

        Map<String, Object> kafkaAdminConfig = new HashMap<>();
        kafkaAdminConfig.put(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaServiceUri.getServiceHosts()[0]
        );
        kafkaAdmin = AdminClient.create(kafkaAdminConfig);
    }

    @Override
    protected void doTeardown() throws Exception {
        if (null != kafkaAdmin) {
            kafkaAdmin.close();
        }

        super.doTeardown();
    }

    private void createKafkaTopic(String topic, int numPartitions) throws Exception {
        kafkaAdmin.createTopics(
            Lists.newArrayList(new NewTopic(topic, numPartitions, (short) 1))).all().get();
    }

    <K, V> KafkaProducer<K, V> newKafkaProducer(Serializer<K> keySerializer,
                                                Serializer<V> valueSerializer) {
        Map<String, Object> kafkaProducerConfigMap = new HashMap<>();
        kafkaProducerConfigMap.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            keySerializer.getClass().getName()
        );
        kafkaProducerConfigMap.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            valueSerializer.getClass().getName()
        );
        kafkaProducerConfigMap.put(
            ProducerConfig.ACKS_CONFIG,
            "1"
        );
        kafkaProducerConfigMap.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaServiceUri.getServiceHosts()[0]
        );

        return new KafkaProducer<>(
            kafkaProducerConfigMap
        );
    }

    KafkaSourceConfig newKafkaSourceConfig() {
        Map<String, Object> kafkaConsumerConfigMap = new HashMap<>();
        kafkaConsumerConfigMap.put(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()
        );
        kafkaConsumerConfigMap.put(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName()
        );
        kafkaConsumerConfigMap.put(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            kafkaServiceUri.getServiceHosts()[0]
        );
        kafkaConsumerConfigMap.put(
            ConsumerConfig.GROUP_ID_CONFIG,
            "test-group"
        );

        Map<String, Object> pulsarProducerConfigMap = new HashMap<>();
        pulsarProducerConfigMap.put(
            "serviceUrl", pulsarService().getEndpoints().getExternal().getPlainTextServiceUrl());

        return new KafkaSourceConfig()
            .kafka(
                new KafkaConsumerConfig()
                    .consumer(kafkaConsumerConfigMap)
            )
            .pulsar(
                new PulsarProducerConfig()
                    .pulsar_web_service_url(
                        pulsarService().getEndpoints().getExternal().getHttpServiceUrl())
                    .client(pulsarProducerConfigMap)
                    .producer(Collections.emptyMap())
            );
    }

    /**
     * Current version doesn't support 1 partition use case.
     */
    @Ignore
    public void testKafkaTopicAutoCreationPulsarTopicCreateIfMissing() throws Exception {
        final String topic = "kafka-topic-auto-creation" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);

        @Cleanup
        KafkaSource source = new KafkaSource();
        source.open(config.toConfigMap(), ctx);

        // check the topic is create automatically
        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(1, metadata.partitions);
    }

    @Test
    public void testCreatePulsarTopicIfMissing() throws Exception {
        final String topic = "test-create-pulsar-topic-if-missing-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);

        final int numPartitions = 4;

        createKafkaTopic(topic, numPartitions);

        @Cleanup
        KafkaSource source = new KafkaSource();
        source.open(config.toConfigMap(), ctx);

        // check the topic is create automatically
        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(numPartitions, metadata.partitions);
    }

    @Test
    public void testCreatePulsarTopicIfMissingDisabled() throws Exception {
        final String topic = "test-create-pulsar-topic-if-missing-disabled-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.pulsar().create_topic_if_missing(false);

        final int numPartitions = 4;

        createKafkaTopic(topic, numPartitions);

        try (KafkaSource source = new KafkaSource()) {
            source.open(config.toConfigMap(), ctx);
            fail("Should fail to start kafka source if create_topic_if_missing is disabled");
        } catch (PulsarAdminException pae) {
            assertTrue(pae instanceof NotFoundException);
        }

        // check the topic is not created
        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(0, metadata.partitions);
    }

    @Test
    public void testAllowDifferentNumPartitions() throws Exception {
        final String topic = "test-allow-different-num-partitions-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.pulsar().create_topic_if_missing(false);
        config.pulsar().allow_different_num_partitions(true);
        config.pulsar().update_partitions_if_inconsistent(false);

        final int numKafkaPartitions = 4;
        final int numPulsarPartitions = 5;

        createKafkaTopic(topic, numKafkaPartitions);
        admin.topics().createPartitionedTopic(topic, numPulsarPartitions);

        KafkaSource source = new KafkaSource();
        source.open(config.toConfigMap(), ctx);

        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(numPulsarPartitions, metadata.partitions);
    }

    @Test
    public void testUpdatePartitionsIfInconsistentWithLessPartitions() throws Exception {
        final String topic = "test-update-partitions-if-inconsistent-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.pulsar().create_topic_if_missing(false);
        config.pulsar().allow_different_num_partitions(true);
        config.pulsar().update_partitions_if_inconsistent(true);

        final int numKafkaPartitions = 8;
        final int numPulsarPartitions = 5;

        createKafkaTopic(topic, numKafkaPartitions);
        admin.topics().createPartitionedTopic(topic, numPulsarPartitions);

        KafkaSource source = new KafkaSource();
        source.open(config.toConfigMap(), ctx);

        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(numKafkaPartitions, metadata.partitions);
    }

    @Test
    public void testUpdatePartitionsIfInconsistentWithMorePartitions() throws Exception {
        final String topic = "test-update-partitions-if-inconsistent-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.pulsar().create_topic_if_missing(false);
        config.pulsar().allow_different_num_partitions(true);
        config.pulsar().update_partitions_if_inconsistent(true);

        final int numKafkaPartitions = 5;
        final int numPulsarPartitions = 8;

        createKafkaTopic(topic, numKafkaPartitions);
        admin.topics().createPartitionedTopic(topic, numPulsarPartitions);

        KafkaSource source = new KafkaSource();
        source.open(config.toConfigMap(), ctx);

        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(numPulsarPartitions, metadata.partitions);
    }

    @Test
    public void testDisallowDifferentNumPartitions() throws Exception {
        final String topic = "test-disallow-different-num-partitions-" + Base58.randomString(8);

        KafkaSourceConfig config = newKafkaSourceConfig();
        config.kafka().topic(topic);
        config.pulsar().create_topic_if_missing(false);
        config.pulsar().allow_different_num_partitions(false);
        config.pulsar().update_partitions_if_inconsistent(false);

        final int numKafkaPartitions = 4;
        final int numPulsarPartitions = 5;

        createKafkaTopic(topic, numKafkaPartitions);
        admin.topics().createPartitionedTopic(topic, numPulsarPartitions);

        try {
            KafkaSource source = new KafkaSource();
            source.open(config.toConfigMap(), ctx);
            fail("Should fail to start kafka source if disallowing different number of partitions");
        } catch (IllegalArgumentException iae) {
            // expected
        }

        PartitionedTopicMetadata metadata = admin.topics().getPartitionedTopicMetadata(topic);
        assertEquals(numPulsarPartitions, metadata.partitions);
    }

    public <K, V> void testKafkaSourceSendAndReceiveMessagesRawKey(
        String kafkaTopic, String pulsarTopic,
        int numPartitions, int numMessages,
        Serializer<K> keySerializer, Serializer<V> valueSerializer,
        Generator<K> keyGenerator, Generator<V> valueGenerator,
        Schema<V> pulsarValueSchema
    ) throws Exception {
        @Cleanup
        Consumer<V> valueConsumer = client.newConsumer(pulsarValueSchema)
            .topic(pulsarTopic)
            .subscriptionName("test-verifier")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        @Cleanup
        KafkaProducer<K, V> kafkaProducer = newKafkaProducer(
            keySerializer, valueSerializer
        );

        // send the messages
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                ProducerRecord<K, V> record = new ProducerRecord<>(
                    kafkaTopic, i,
                    (j + 1) * 1000L,
                    keyGenerator.apply(i, j),
                    valueGenerator.apply(i, j)
                );
                kafkaProducer.send(record);
            }
        }
        kafkaProducer.flush();

        // consume the messages
        Map<Integer, Integer> partitionIdxs = new HashMap<>();
        IntStream.range(0, numPartitions * numMessages).forEach(ignored -> {
            try {
                Message<V> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(
                    TopicName.get(pulsarTopic).toString(),
                    tn.getPartitionedTopicName()
                );
                int partitionIdx = tn.getPartitionIndex();
                int messageIdxInPtn = partitionIdxs.computeIfAbsent(partitionIdx, p -> -1);

                assertEquals((messageIdxInPtn + 1) * 1000L, message.getEventTime());
                byte[] keyData = keySerializer.serialize(
                    kafkaTopic,
                    keyGenerator.apply(partitionIdx, messageIdxInPtn));
                assertArrayEquals(keyData, message.getKeyBytes());

                if (message.getValue() instanceof byte[]) {
                    assertArrayEquals(
                        (byte[]) valueGenerator.apply(partitionIdx, messageIdxInPtn),
                        (byte[]) message.getValue()
                    );
                } else {
                    assertEquals(
                        valueGenerator.apply(partitionIdx, messageIdxInPtn),
                        message.getValue()
                    );
                }

            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }

        });
    }


}
