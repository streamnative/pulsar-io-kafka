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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import lombok.Data;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Kafka source config.
 */
@Data
@Accessors(fluent = true)
public class KafkaSourceConfig {

    /**
     * The configuration for kafka consumer.
     */
    @Data
    @Accessors(fluent = true)
    static class KafkaConsumerConfig {
        @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Kafka topic to consume.")
        private String topic;
        //CHECKSTYLE.OFF: MemberName
        @FieldDoc(
            defaultValue = "1000",
            help = "The Kafka consumer poll duration in milliseconds.")
        private long poll_duration_ms = 1000;
        //CHECKSTYLE.ON: MemberName
        @FieldDoc(
            defaultValue = "",
            help = "The consumer config properties to be passed to a Kafka consumer.")
        private Map<String, Object> consumer;
        @FieldDoc(
            defaultValue = "",
            help = "The kafka schema registry properties")
        private Map<String, Object> schema;

        public void validate() {
            Objects.requireNonNull(
                topic,
                "The Kafka topic is missing");
            Objects.requireNonNull(
                consumer,
                "The Kafka consumer properties are missing");
        }
    }

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The configuration for kafka consumer")
    private KafkaConsumerConfig kafka;

    /**
     * The configuration for pulsar producer.
     */
    @Data
    @Accessors(fluent = true)
    static class PulsarProducerConfig {
        @FieldDoc(
            defaultValue = "",
            help = "The topic to store the pulsar topic. If it is not set, the source connector uses"
                + " the Kafka topic name as the Pulsar topic name"
        )
        private String topic;
        //CHECKSTYLE.OFF: MemberName
        @FieldDoc(
            defaultValue = "false",
            help = "Flag to allow pulsar topic have different number of partitions from kafka topic"
        )
        private boolean allow_different_num_partitions = false;
        @FieldDoc(
            defaultValue = "true",
            help = "Flag to create the pulsar topic if it is missing"
        )
        private boolean create_topic_if_missing = true;
        @FieldDoc(
            defaultValue = "false",
            help = "Flag to control whether to copy kafka schema to pulsar schema"
        )
        private boolean copy_kafka_schema = false;
        //CHECKSTYLE.ON: MemberName
        @FieldDoc(
            defaultValue = "",
            help = "The pulsar client configuration")
        private Map<String, Object> client;
        @FieldDoc(
            defaultValue = "",
            help = "The pulsar producer configuration")
        private Map<String, Object> producer;

        public void validate() {
            Objects.requireNonNull(
                client,
                "The Pulsar client settings are missing");
        }
    }

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The configuration for pulsar producer")
    private PulsarProducerConfig pulsar;

    public static KafkaSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), KafkaSourceConfig.class);
    }

    public static KafkaSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KafkaSourceConfig.class);
    }

}
