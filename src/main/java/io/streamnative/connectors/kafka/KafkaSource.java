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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.streamnative.connectors.kafka.KafkaSourceConfig.KafkaConsumerConfig;
import io.streamnative.connectors.kafka.pulsar.MultiVersionKeyValueSchemaProducer;
import io.streamnative.connectors.kafka.pulsar.PulsarProducer;
import io.streamnative.connectors.kafka.pulsar.RawKeySchemaValueProducer;
import io.streamnative.connectors.kafka.schema.KafkaBytesSchema;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
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
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConfigurationDataUtils;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.schema.StringSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;

/**
 * Kafka source connector.
 */
@Slf4j
public class KafkaSource implements Source<byte[]> {

    @Data
    @Accessors(fluent = true)
    private static class PendingMessage {
        final ConsumerRecord<Object, Object> record;
        final CompletableFuture<MessageId> writeFuture;
    }

    public static final String HEADER_KAFKA_TOPIC_KEY = "__kafka_topic";
    public static final String HEADER_KAFKA_PTN_KEY = "__kafka_partition";
    public static final String HEADER_KAFKA_OFFSET_KEY = "__kafka_offset";

    private volatile boolean running = false;
    private KafkaSourceConfig config;
    private Thread runnerThread;

    // consumer
    private Consumer<Object, Object> kafkaConsumer;

    // producer
    private PulsarClient pulsarClient;
    private PulsarProducer pulsarProducer;

    @Override
    public void open(Map<String, Object> map,
                     SourceContext sourceContext) throws Exception {
        this.config = KafkaSourceConfig.load(map);

        Objects.requireNonNull(
            config.kafka(),
            "The Kafka settings are missing");
        config.kafka().validate();

        Objects.requireNonNull(
            config.pulsar(),
            "The Pulsar settings are missing");
        config.pulsar().validate();

        Schema keySchema = getPulsarSchemaAndReconfigureDeserializerClass(
            config,
            true
        );
        Schema valueSchema = getPulsarSchemaAndReconfigureDeserializerClass(
            config,
            false
        );

        // create the pulsar client
        this.pulsarClient = PulsarClient.builder()
            .loadConf(config.pulsar().client())
            .build();

        this.kafkaConsumer = initializeKafkaConsumer(config.kafka());

        // prepare the metadata
        List<PartitionInfo> kafkaPartitions = kafkaConsumer.partitionsFor(config.kafka().topic());

        final String pulsarTopic = getPulsarTopic(config);
        try {
            List<String> pulsarPartitions = pulsarClient.getPartitionsForTopic(pulsarTopic).get();
            if (kafkaPartitions.size() != pulsarPartitions.size()
                && !config.pulsar().allow_different_num_partitions()) {
                throw new IllegalArgumentException(
                    "Inconsistent partition number : Kafka topic '" + config.kafka().topic()
                        + "' has " + kafkaPartitions.size() + " partitions but Pulsar topic '"
                        + pulsarTopic + "' has " + pulsarPartitions.size() + " partitions"
                );
            }
        } catch (ExecutionException ee) {
            // TODO: handle partitioned topic not exist
            throw ee;
        }

        ProducerConfigurationData producerConf = new ProducerConfigurationData();
        if (config.pulsar().producer() != null) {
            ConfigurationDataUtils.loadData(config.pulsar().producer(), producerConf, ProducerConfigurationData.class);
        }
        MessageRouter messageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            producerConf.isBatchingEnabled(),
            TimeUnit.MICROSECONDS.toMillis(producerConf.getBatchingMaxPublishDelayMicros())
        );

        if (keySchema == null) {
            this.pulsarProducer = new RawKeySchemaValueProducer(
                pulsarClient,
                pulsarTopic,
                valueSchema == null ? Schema.BYTES : valueSchema,
                config.pulsar().producer(),
                messageRouter
            );
        } else {
            KafkaSchemaManager kafkaSchemaManager = new KafkaSchemaManager(null, config.kafka().schema());
            this.pulsarProducer = new MultiVersionKeyValueSchemaProducer(
                pulsarClient,
                pulsarTopic,
                keySchema,
                valueSchema == null ? Schema.BYTES : valueSchema,
                config.pulsar().producer(),
                messageRouter,
                kafkaSchemaManager
            );
        }

        // start the Kafka fetch thread
        start();
    }

    private static String getPulsarTopic(KafkaSourceConfig config) {
        if (config.pulsar().topic() == null) {
            return config.kafka().topic();
        } else {
            return config.pulsar().topic();
        }
    }

    private <K, V> Consumer<K, V> initializeKafkaConsumer(KafkaConsumerConfig config) {
        Properties props = new Properties();
        props.putAll(config.consumer());

        return new KafkaConsumer<>(props);
    }

    private Class<? extends Deserializer> getKafkaDeserializerClass(Map<String, Object> props,
                                                                    String deserializerClassKey)
        throws ClassNotFoundException {
        Object deserializerClassName = props.get(deserializerClassKey);
        Objects.requireNonNull(
            deserializerClassName, "Deserializer class for '" + deserializerClassKey + "' is not set");
        if (!(deserializerClassName instanceof String)) {
            throw new IllegalArgumentException("Unknown deserializer class value for '" + deserializerClassKey + "'");
        }

        Class<?> theCls = Class.forName((String) deserializerClassName);
        if (!Deserializer.class.isAssignableFrom(theCls)) {
            throw new IllegalArgumentException("Class " + theCls + " is not a Kafka deserializer");
        }
        return theCls.asSubclass(Deserializer.class);
    }

    private Schema getPulsarSchemaAndReconfigureDeserializerClass(KafkaSourceConfig config,
                                                                  boolean isKey) throws ClassNotFoundException {
        String deserializerClassKey;
        if (isKey) {
            deserializerClassKey = ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
        } else {
            deserializerClassKey = ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
        }

        if (!config.pulsar().copy_kafka_schema()) {
            // if we don't copy kafka schema, then just transfer the raw bytes.
            config.kafka().consumer().put(
                deserializerClassKey,
                ByteArrayDeserializer.class.getName()
            );
            return null;
        }

        Class<? extends Deserializer> kafkaDeserializerClass = getKafkaDeserializerClass(
            config.kafka().consumer(),
            deserializerClassKey
        );

        if (ByteArrayDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.BYTES;
        } else if (ByteBufferDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.BYTEBUFFER;
        } else if (BytesDeserializer.class.equals(kafkaDeserializerClass)) {
            return KafkaBytesSchema.of();
        } else if (StringDeserializer.class.equals(kafkaDeserializerClass)) {
            return new StringSchema(getEncodingCharset(config.kafka().consumer(), isKey));
        } else if (DoubleDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.DOUBLE;
        } else if (FloatDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.FLOAT;
        } else if (IntegerDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.INT32;
        } else if (LongDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.INT64;
        } else if (ShortDeserializer.class.equals(kafkaDeserializerClass)) {
            return Schema.INT16;
        } else if (KafkaAvroDeserializer.class.equals(kafkaDeserializerClass)) {
            if (config.kafka().schema() == null) {
                // if kafka schema registry is configured, just transfer the raw bytes.
                log.info("Kafka schema registry is not defined. Configure `"
                    + deserializerClassKey + "` to " + ByteArrayDeserializer.class.getName());
                config.kafka().consumer().put(
                    deserializerClassKey,
                    ByteArrayDeserializer.class.getName()
                );
                return null;
            } else {
                // replace this deserializer to KafkaSchemaAndBytesDeserializer
                log.info("Kafka schema registry is defined. Reconfigure `"
                    + deserializerClassKey + "` to " + KafkaSchemaAndBytesDeserializer.class.getName());
                config.kafka().consumer().put(
                    deserializerClassKey,
                    KafkaSchemaAndBytesDeserializer.class.getName());
                return new KafkaAvroSchema();
            }
        } else {
            log.info("Unknown Kafka deserializer. Configure `" + deserializerClassKey + "` to "
                + ByteArrayDeserializer.class.getName() + " to just transfer raw bytes.");
            config.kafka().consumer().put(
                deserializerClassKey,
                ByteArrayDeserializer.class.getName()
            );
            return null;
        }
    }

    private static Charset getEncodingCharset(Map<String, ?> configs, boolean isKey) {
        Charset charset = UTF_8;
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null) {
            encodingValue = configs.get("deserializer.encoding");
        }
        if (encodingValue instanceof String) {
            charset = Charset.forName((String) encodingValue);
        }
        return charset;
    }

    private void start() {
        runnerThread = new Thread(() -> {
            log.info("Starting Kafka source to consume messages from Kafka topic {} ...", config.kafka().topic());
            kafkaConsumer.subscribe(Collections.singletonList(config.kafka().topic()));
            log.info("Kafka source started.");
            try {
                consumeKafkaRecordsLoop();
            } catch (InterruptedException ie) {
                log.warn("The KafkaFetchThread is interrupted, exiting ...");
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.warn("The KafkaFetchThread encountered exception, exiting", e);
                return;
            }
        }, "KafkaFetchThread");
        runnerThread.setUncaughtExceptionHandler((t, e) ->
            log.error("[{}] Encountered uncaught exception", t.getName(), e));
        runnerThread.start();
    }

    private void consumeKafkaRecordsLoop() throws InterruptedException, ExecutionException {
        ConsumerRecords<Object, Object> consumerRecords;
        LinkedBlockingQueue<PendingMessage> pendingMessages = new LinkedBlockingQueue<>();
        while (running) {
            consumerRecords = kafkaConsumer.poll(Duration.ofMillis(config.kafka().poll_duration_ms()));
            Iterator<ConsumerRecord<Object, Object>> recordIterator = consumerRecords.iterator();
            while (recordIterator.hasNext()) {
                ConsumerRecord<Object, Object> record = recordIterator.next();
                CompletableFuture<MessageId> sendFuture = pulsarProducer.send(record);
                PendingMessage pendingMessage = new PendingMessage(record, sendFuture);
                pendingMessages.put(pendingMessage);
            }
            PendingMessage msg = pendingMessages.peek();
            Map<TopicPartition, Long> offsets = new HashMap<>();
            while (msg != null && msg.writeFuture().isDone()) {
                msg.writeFuture.get();

                ConsumerRecord<Object, Object> record = msg.record;
                offsets.put(
                    new TopicPartition(record.topic(), record.partition()),
                    record.offset()
                );

                pendingMessages.poll();
                msg = pendingMessages.peek();
            }

            if (!offsets.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = offsets.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new OffsetAndMetadata(e.getValue())
                    ));
                kafkaConsumer.commitAsync(offsetAndMetadataMap, (offsetsMap, exception) -> {
                    if (null != exception) {
                        // we can still continue because the data can still be written to pulsar
                        log.warn("Failed to commit Kafka offsets {}", offsets, exception);
                    }
                });
            }
        }

    }


    @Override
    public Record<byte[]> read() throws Exception {
        if (runnerThread != null) {
            runnerThread.join();
        }
        throw new InterruptedException("The KafkaFetchThread exits");
    }

    @Override
    public void close() throws Exception {
        log.info("Stopping Kafka source ...");
        running = false;
        if (runnerThread != null) {
            runnerThread.interrupt();
            runnerThread.join();
            runnerThread = null;
        }
        if (null != kafkaConsumer) {
            kafkaConsumer.close();
        }
        if (null != pulsarProducer) {
            pulsarProducer.close();
        }
        if (null != pulsarClient) {
            pulsarClient.close();
        }
        log.info("Kafka source successfully stopped");
    }
}
