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
package io.streamnative.connectors.kafka.example;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.pulsar.client.api.Schema;

/**
 * Example to produce json records.
 */
public class KafkaJsonProducerExample {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: KafkaJsonProducerExample <bootstrap-server> <topic> <num>");
            return;
        }

        String serviceUrl = args[0];
        String topic = args[1];
        int numMessages = Integer.parseInt(args[2]);

        Map<String, Object> config = new HashMap<>();
        config.put(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serviceUrl);
        config.put(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(config)) {
            Schema<User> schema = Schema.JSON(User.class);

            for (int i = 0; i < numMessages; i++) {
                User user = new User(
                    "user-" + i,
                    10 * i,
                    "address-" + i
                );
                byte[] keyBytes = ("user-" + i).getBytes(UTF_8);
                byte[] valBytes = schema.encode(user);

                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                    topic, keyBytes, valBytes
                );

                CompletableFuture<RecordMetadata> sendFuture = new CompletableFuture<>();
                producer.send(record, (metadata, exception) -> {
                    if (null != exception) {
                        sendFuture.completeExceptionally(exception);
                    } else {
                        sendFuture.complete(metadata);
                    }
                }).get();

                RecordMetadata recordMetadata = sendFuture.get();
                System.out.println("Successfully send " + i + "-th message : value = "
                    + user + ", offset = " + recordMetadata.offset());
            }
            System.out.println("Successfully send " + numMessages + " messages to topic " + topic);
        }
    }

}
