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
package io.streamnative.connectors.kafka.pulsar;

import io.streamnative.connectors.kafka.KafkaSource;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

/**
 * The producer base implementation.
 */
@SuppressWarnings("unchecked")
public abstract class PulsarProducerBase implements PulsarProducer {

    @FunctionalInterface
    public interface ExceptionalFunction<I, O> {

        O apply(I input) throws PulsarClientException;

    }

    protected final Schema schema;
    protected final Producer producer;

    @SuppressWarnings("unchecked")
    protected PulsarProducerBase(PulsarClient client,
                                 String topic,
                                 Schema schema,
                                 Map<String, Object> producerConfig,
                                 MessageRouter messageRouter) throws PulsarClientException {
        this(
            schema,
            s -> client.newProducer(s)
                .loadConf(producerConfig)
                .topic(topic)
                .messageRouter(messageRouter)
                .create());
    }

    protected PulsarProducerBase(Schema schema,
                                 ExceptionalFunction<Schema, Producer> producerFunc) throws PulsarClientException {
        this.schema = schema;
        this.producer = producerFunc.apply(schema);
    }

    protected TypedMessageBuilder newMessage(ConsumerRecord record) {
        TypedMessageBuilder msgBuilder = producer.newMessage()
            .sequenceId(record.offset())
            .property(KafkaSource.HEADER_KAFKA_TOPIC_KEY, record.topic())
            .property(KafkaSource.HEADER_KAFKA_PTN_KEY, Integer.toString(record.partition()))
            .property(KafkaSource.HEADER_KAFKA_OFFSET_KEY, Long.toString(record.offset()));

        if (record.timestampType() == TimestampType.CREATE_TIME) {
            msgBuilder = msgBuilder.eventTime(record.timestamp());
        }

        return msgBuilder;
    }

    @Override
    public void close() throws Exception {
        producer.close();
    }
}
