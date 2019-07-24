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

import static io.streamnative.connectors.kafka.AvroTestSchemas.USER_SCHEMA;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

/**
 * Unit test {@link KafkaAvroSchema}.
 */
@SuppressWarnings("unchecked")
public class KafkaAvroSchemaTest {

    private final String topic;
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaSchemaAndBytesDeserializer ksBytesDeserializer;
    private final KafkaAvroSerializer kafkaAvroSerializer;
    private final KafkaAvroSchema kafkaAvroSchema;

    public KafkaAvroSchemaTest() {
        this.topic = "test";
        this.schemaRegistry = new MockSchemaRegistryClient();
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        this.kafkaAvroSerializer = new KafkaAvroSerializer(schemaRegistry, new HashMap(defaultConfig));
        this.ksBytesDeserializer = new KafkaSchemaAndBytesDeserializer();
        this.kafkaAvroSchema = new KafkaAvroSchema();
        this.kafkaAvroSchema.setAvroSchema(USER_SCHEMA);
    }

    private IndexedRecord createAvroRecord(String name) {
        GenericRecord avroRecord = new GenericData.Record(USER_SCHEMA);
        avroRecord.put("name", name);
        return avroRecord;
    }

    @Test
    public void testGetSchemaInfo() {
        SchemaInfo schemaInfo = this.kafkaAvroSchema.getSchemaInfo();
        assertEquals("KafkaAvro", schemaInfo.getName());
        assertEquals(SchemaType.AVRO, schemaInfo.getType());
        assertTrue(schemaInfo.getProperties().isEmpty());
        assertEquals(
            USER_SCHEMA,
            new Schema.Parser().parse(schemaInfo.getSchemaDefinition())
        );
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

        ByteBuffer duplicated = ksBytes.getData().duplicate();
        byte[] avroData = new byte[duplicated.remaining()];
        duplicated.get(avroData);
        byte[] encodedData = kafkaAvroSchema.encode(ksBytes);
        assertArrayEquals(avroData, encodedData);
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

        ByteBuffer duplicated = ksBytes.getData().duplicate();
        byte[] avroData = new byte[duplicated.remaining()];
        duplicated.get(avroData);
        byte[] encodedData = kafkaAvroSchema.encode(ksBytes);
        assertArrayEquals(avroData, encodedData);
    }

}
