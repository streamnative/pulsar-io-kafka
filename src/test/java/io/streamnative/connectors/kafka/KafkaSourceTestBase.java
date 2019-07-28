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

import static io.streamnative.tests.pulsar.service.ExternalServices.KAFKA;
import static io.streamnative.tests.pulsar.service.ExternalServices.KAFKA_SCHEMA_REGISTRY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.common.collect.Lists;
import io.streamnative.connectors.kafka.KafkaSourceConfig.KafkaConsumerConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.PulsarProducerConfig;
import io.streamnative.connectors.kafka.pulsar.Generator;
import io.streamnative.tests.common.framework.Service;
import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.net.ServiceURI;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.io.core.SourceContext;

/**
 * A test base for running kafka source tests.
 */
@Slf4j
public abstract class KafkaSourceTestBase extends PulsarServiceSystemTestCase {

    protected Service kafkaService;
    protected Service schemaRegistryService;
    protected ServiceURI kafkaServiceUri;
    protected ServiceURI schemaRegistryServiceUri;

    protected final SourceContext ctx;
    protected AdminClient kafkaAdmin;

    public KafkaSourceTestBase(PulsarService service) {
        super(service);
        this.ctx = mock(SourceContext.class);
    }

    @Override
    protected Consumer<byte[]> createConsumer(String topic, String subscription) throws PulsarClientException {
        return super.createConsumer(topic, subscription);
    }

    @Override
    protected void doSetup() throws Exception {
        super.doSetup();

        this.kafkaService = pulsarService().getExternalServiceOperator()
            .getService(KAFKA);
        this.schemaRegistryService = pulsarService().getExternalServiceOperator()
            .getService(KAFKA_SCHEMA_REGISTRY);

        this.kafkaServiceUri = ServiceURI.create(kafkaService.getServiceUris().get(0));
        this.schemaRegistryServiceUri = ServiceURI.create(schemaRegistryService.getServiceUris().get(0));

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

    protected static <T> Schema<T> disabledSchema(Schema<T> schema) {
        return new Schema<T>() {
            @Override
            public byte[] encode(T t) {
                return schema.encode(t);
            }

            @Override
            public SchemaInfo getSchemaInfo() {
                return Schema.BYTES.getSchemaInfo();
            }

            @Override
            public void validate(byte[] message) {
                schema.validate(message);
            }

            @Override
            public boolean supportSchemaVersioning() {
                return schema.supportSchemaVersioning();
            }

            @Override
            public void setSchemaInfoProvider(SchemaInfoProvider schemaInfoProvider) {
                schema.setSchemaInfoProvider(schemaInfoProvider);
            }

            @Override
            public T decode(byte[] bytes) {
                return schema.decode(bytes);
            }

            @Override
            public T decode(byte[] bytes, byte[] schemaVersion) {
                return schema.decode(bytes, schemaVersion);
            }
        };
    }

    protected void createKafkaTopic(String topic, int numPartitions) throws Exception {
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
        kafkaProducerConfigMap.put(
            KafkaSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryServiceUri.getUri().toString()
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

    protected <K, V> void testKafkaSourceSendAndReceiveMessagesRawKey(
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
                CompletableFuture<RecordMetadata> sendFuture = new CompletableFuture<>();
                kafkaProducer.send(record, (metadata, exception) -> {
                    if (null != exception) {
                        sendFuture.completeExceptionally(exception);
                    } else {
                        sendFuture.complete(metadata);
                    }
                }).get();
                RecordMetadata metadata = sendFuture.get();
                log.info("Send message to Kafka topic {} : ({}, {}) - offset: {}",
                    kafkaTopic, i, j, metadata.offset());
            }
        }
        kafkaProducer.flush();

        // consume the messages
        Map<Integer, Integer> partitionIdxs = new HashMap<>();
        AtomicInteger totalReceived = new AtomicInteger(0);
        IntStream.range(0, numPartitions * numMessages).forEach(ignored -> {
            try {
                Message<V> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(
                    TopicName.get(pulsarTopic).toString(),
                    tn.getPartitionedTopicName()
                );
                int partitionIdx = tn.getPartitionIndex();
                int messageIdxInPtn = partitionIdxs.computeIfAbsent(partitionIdx, p -> -1) + 1;
                log.info("Receive {} message from partition {} : total = {}",
                    messageIdxInPtn, partitionIdx, totalReceived.incrementAndGet());

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

                partitionIdxs.put(partitionIdx, messageIdxInPtn);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }

        });
    }

}
