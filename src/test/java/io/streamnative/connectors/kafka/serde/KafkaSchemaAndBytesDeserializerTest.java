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
package io.streamnative.connectors.kafka.serde;

import static io.streamnative.connectors.kafka.AvroTestSchemas.USER_SCHEMA;
import static io.streamnative.connectors.kafka.AvroTestSchemas.USER_SCHEMA_DEF;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Unit test {@link KafkaSchemaAndBytesDeserializer}.
 */
@SuppressWarnings("unchecked")
public class KafkaSchemaAndBytesDeserializerTest {

    private final String topic;
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaSchemaAndBytesDeserializer ksBytesDeserializer;
    private final Schema<org.apache.pulsar.client.api.schema.GenericRecord> pulsarSchema;
    private final KafkaAvroSerializer kafkaAvroSerializer;

    public KafkaSchemaAndBytesDeserializerTest() {
        this.topic = "test";
        this.schemaRegistry = new MockSchemaRegistryClient();
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        this.kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        this.ksBytesDeserializer = new KafkaSchemaAndBytesDeserializer();
        this.pulsarSchema = Schema.generic(SchemaInfo.builder()
            .type(SchemaType.AVRO)
            .name("AVRO")
            .schema(USER_SCHEMA_DEF.getBytes(UTF_8))
            .properties(Collections.emptyMap())
            .build());
    }

    private IndexedRecord createAvroRecord(String name) {
        GenericRecord avroRecord = new GenericData.Record(USER_SCHEMA);
        avroRecord.put("name", name);
        return avroRecord;
    }

    @Test
    public void testDeserializeAvroKeyRecord() throws Exception {
        final String name = "testUser";

        Map<String, Object> configs = new HashMap<>();
        configs.put(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus"
        );
        configs.put(
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
            true
        );
        kafkaAvroSerializer.configure(configs, false);

        IndexedRecord avroRecord = createAvroRecord(name);
        int kafkaSchemaId = schemaRegistry.register(topic + "-key", avroRecord.getSchema());
        byte[] bytes = kafkaAvroSerializer.serialize(topic, avroRecord);

        KafkaSchemaAndBytes ksBytes = ksBytesDeserializer.deserialize(topic, bytes);
        assertEquals(kafkaSchemaId, ksBytes.getSchemaId());

        byte[] avroData = new byte[ksBytes.getData().remaining()];
        ksBytes.getData().get(avroData);
        org.apache.pulsar.client.api.schema.GenericRecord pulsarGenericRecord = pulsarSchema.decode(avroData);
        assertEquals(name, pulsarGenericRecord.getField("name"));
    }

    @Test
    public void testDeserializeAvroValueRecord() throws Exception {
        final String name = "testUser";

        Map<String, Object> configs = new HashMap<>();
        configs.put(
            KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "bogus"
        );
        configs.put(
            KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS,
            false
        );
        kafkaAvroSerializer.configure(configs, false);

        IndexedRecord avroRecord = createAvroRecord(name);
        int kafkaSchemaId = schemaRegistry.register(topic + "-value", avroRecord.getSchema());
        byte[] bytes = kafkaAvroSerializer.serialize(topic, avroRecord);

        KafkaSchemaAndBytes ksBytes = ksBytesDeserializer.deserialize(topic, bytes);
        assertEquals(kafkaSchemaId, ksBytes.getSchemaId());

        byte[] avroData = new byte[ksBytes.getData().remaining()];
        ksBytes.getData().get(avroData);
        org.apache.pulsar.client.api.schema.GenericRecord pulsarGenericRecord = pulsarSchema.decode(avroData);
        assertEquals(name, pulsarGenericRecord.getField("name"));
    }

    @Test
    public void testDeserializeNullRecord() {
        assertNull(ksBytesDeserializer.deserialize(topic, null));
    }
}
