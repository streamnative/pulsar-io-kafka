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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.io.core.annotations.FieldDoc;

/**
 * Kafka source config.
 */
@Data
@Accessors(fluent = true)
@AllArgsConstructor
@NoArgsConstructor
@Slf4j
public class KafkaSourceConfig {

    /**
     * The type of json schema provider.
     */
    public enum JsonSchemaProvider {

        // the schema is nested in the json content. the json data generated from
        // Kafka Connect usually carries the schema.
        INLINE,

        // the schema is provided in the config. the schema is a 'json' string
        // written in Avro schema specification.
        CONFIG,

        // the schema is provided by the Pulsar topic.
        PULSAR

    }

    /**
     * Converter type.
     */
    public enum ConverterType {
        JSON,
        AVRO
    }

    /**
     * The configuration for Kafka schema.
     */
    @Data
    @Accessors(fluent = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaSchemaConfig {

        //CHECKSTYLE.OFF: MemberName
        @FieldDoc(
            defaultValue = "",
            help = "The kafka schema registry properties")
        public Map<String, Object> schema_registry;

        @FieldDoc(
            defaultValue = "CONFIG",
            help = "The json schema provider type. Available types : INLINE, CONFIG, PULSAR"
        )
        public JsonSchemaProvider json_schema_provider = JsonSchemaProvider.CONFIG;

        @FieldDoc(
            defaultValue = "",
            help = "The Kafka converter to convert the value data"
        )
        public ConverterType value_converter;

        @FieldDoc(
            defaultValue = "",
            help = "The value schema definition written in json format using Avro specification."
                + " This value schema definition is only used when `json_schema_provider`"
                + " is configured to `CONFIG` provider"
        )
        public String value_schema;

        @FieldDoc(
            defaultValue = "",
            help = "The Kafka converter to convert the key data"
        )
        public ConverterType key_converter;

        @FieldDoc(
            defaultValue = "",
            help = "The key schema definition written in json format using Avro specification."
                + " This key schema definition is only used when `json_schema_provider`"
                + " is configured to `CONFIG` provider"
        )
        public String key_schema;

        @FieldDoc(
            defaultValue = "true",
            help = "If the records read from Kafka topic are not able to be converted to the"
                + " target schema, skip the records."
        )
        public boolean skip_corrupted_records = true;
        //CHECKSTYLE.ON: MemberName
    }

    @SuppressWarnings("unchecked")
    public Map<String, Object> toConfigMap() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        String json = mapper.writeValueAsString(this);
        log.info("Serialize kafka source config {} to json {}", this, json);
        return mapper.readValue(json, Map.class);
    }

    /**
     * The configuration for kafka consumer.
     */
    @Data
    @Accessors(fluent = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class KafkaConsumerConfig {
        @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The Kafka topic to consume.")
        public String topic;
        //CHECKSTYLE.OFF: MemberName
        @FieldDoc(
            defaultValue = "1000",
            help = "The Kafka consumer poll duration in milliseconds.")
        public long poll_duration_ms = 1000;
        //CHECKSTYLE.ON: MemberName
        @FieldDoc(
            defaultValue = "",
            help = "The consumer config properties to be passed to a Kafka consumer.")
        public Map<String, Object> consumer;

        @FieldDoc(
            defaultValue = "",
            help = "The config properties for interpreting Kafka schema")
        public KafkaSchemaConfig schema = new KafkaSchemaConfig();

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
    @JsonProperty
    public KafkaConsumerConfig kafka;

    /**
     * The configuration for pulsar producer.
     */
    @Data
    @Accessors(fluent = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class PulsarProducerConfig {
        @FieldDoc(
            defaultValue = "",
            help = "The topic to store the pulsar topic. If it is not set, the source connector uses"
                + " the Kafka topic name as the Pulsar topic name"
        )
        public String topic;
        //CHECKSTYLE.OFF: MemberName
        @FieldDoc(
            required = true,
            defaultValue = "",
            help = "The pulsar web service url used for accessing Pulsar topic metadata and creating Pulsar topic"
        )
        public String pulsar_web_service_url;
        @FieldDoc(
            defaultValue = "false",
            help = "Flag to allow pulsar topic have different number of partitions from kafka topic"
        )
        public boolean allow_different_num_partitions = false;
        @FieldDoc(
            defaultValue = "true",
            help = "Flag to update the num of partitions for Pulsar topic if it is inconsistent to Kafka topic."
                + " Currently it only supports increasing the number of partitions but not decreasing the number"
                + " of partitions"
        )
        public boolean update_partitions_if_inconsistent = true;
        @FieldDoc(
            defaultValue = "true",
            help = "Flag to create the pulsar topic if it is missing"
        )
        public boolean create_topic_if_missing = true;
        @FieldDoc(
            defaultValue = "false",
            help = "Flag to control whether to copy kafka schema to pulsar schema"
        )
        public boolean copy_kafka_schema = false;
        //CHECKSTYLE.ON: MemberName
        @FieldDoc(
            defaultValue = "",
            help = "The pulsar client configuration")
        public Map<String, Object> client;
        @FieldDoc(
            defaultValue = "",
            help = "The pulsar producer configuration")
        public Map<String, Object> producer = Collections.emptyMap();

        public void validate() {
            Objects.requireNonNull(
                pulsar_web_service_url,
                "The Pulsar web service url is missing");
            Objects.requireNonNull(
                client,
                "The Pulsar client settings are missing");
        }
    }

    @FieldDoc(
        required = true,
        defaultValue = "",
        help = "The configuration for pulsar producer")
    public PulsarProducerConfig pulsar;

    public static KafkaSourceConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper.readValue(new File(yamlFile), KafkaSourceConfig.class);
    }

    public static KafkaSourceConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper.readValue(new ObjectMapper().writeValueAsString(map), KafkaSourceConfig.class);
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize kafka source config");
        }
    }

}
