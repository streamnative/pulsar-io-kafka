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
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;

/**
 * Example to consume avro records.
 */
public class PulsarAvroConsumerExample {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: PulsarAvroConsumerExample <service-url> <topic> <sub>");
            return;
        }

        String serviceUrl = args[0];
        String topic = args[1];
        String subscription = args[2];

        try (PulsarClient client = PulsarClient.builder()
             .serviceUrl(serviceUrl)
             .build()) {

            try (Consumer<User> consumer = client.newConsumer(Schema.AVRO(User.class))
                 .topic(topic)
                 .subscriptionName(subscription)
                 .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                 .subscribe()) {
                while (true) {
                    Message<User> msg = consumer.receive();
                    String key = new String(msg.getKeyBytes(), UTF_8);
                    User value = msg.getValue();

                    System.out.println("Receive message : key = " + key + ", value = " + value);
                }
            }

        }

    }

}
