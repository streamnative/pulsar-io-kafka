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
package io.streamnative.connectors.kafka.schema;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import io.confluent.connect.avro.AvroData;
import io.streamnative.connectors.kafka.KafkaConnectDataDeserializer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Json Schema.
 */
@Slf4j
public class KafkaJsonSchemaTest {

    /**
     * Test Struct.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TestStruct {
        public int intField;
        public String strField;
    }

    private final KafkaJsonSchema jsonSchema;
    private final KafkaConnectDataDeserializer deserializer;
    private final AvroData avroData;

    public KafkaJsonSchemaTest() {
        deserializer = new KafkaConnectDataDeserializer();
        jsonSchema = new KafkaJsonSchema();
        avroData = new AvroData(1000);
    }

    @Before
    public void setup() {
        deserializer.configure(Collections.emptyMap(), false);
    }

    @Test(expected = DataException.class)
    public void testJsonDataWithoutSchema() throws Exception {
        TestStruct testStruct = new TestStruct(
            32,
            "value-32"
        );
        byte[] dataBytes = ObjectMapperFactory.getThreadLocal().writeValueAsBytes(testStruct);
        deserializer.deserialize("test-topic", dataBytes);
    }

    @Test
    public void testJsonConverter() throws Exception {
        Schema schema = SchemaBuilder.struct()
            .field("intField", SchemaBuilder.int32().optional())
            .field("strField", SchemaBuilder.string().optional())
            .build();
        Struct struct = new Struct(schema)
            .put("intField", 32)
            .put("strField", "value-32");
        SchemaAndValue data = new SchemaAndValue(
            schema,
            struct
        );

        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(data.schema());
        log.info("Avro Schema : {}", avroSchema);

        // write to json bytes
        jsonSchema.setAvroSchema(deserializer.getConverter(), null, avroSchema);
        byte[] jsonBytes = jsonSchema.encode(data);

        // deserialize json bytes to `SchemaAndValue`
        SchemaAndValue value = deserializer.deserialize("test-topic", jsonBytes);
        assertEquals(
            "\nExpect schema : " + avroData.fromConnectSchema(schema) + "\n"
                + "Actual schema : " + avroData.fromConnectSchema(value.schema()),
            avroData.fromConnectSchema(schema),
            avroData.fromConnectSchema(value.schema())
        );
        Struct actualStruct = (Struct) value.value();
        assertEquals(
            32, actualStruct.getInt32("intField").intValue());
        assertEquals(
            "value-32", actualStruct.getString("strField"));

        // encode connect data without schema
        JsonConverter converterWithoutSchema = new JsonConverter();
        Map<String, Object> config = new HashMap<>();
        config.put("schemas.enable", false);
        config.put("converter.type", ConverterType.VALUE.getName());
        converterWithoutSchema.configure(config);
        jsonSchema.setAvroSchema(
            converterWithoutSchema, null, avroSchema
        );
        byte[] jsonBytesWithoutSchema = jsonSchema.encode(value);
        TestStruct testStruct = ObjectMapperFactory.getThreadLocal()
            .readValue(jsonBytesWithoutSchema, TestStruct.class);
        assertEquals(32, testStruct.intField);
        assertEquals("value-32", testStruct.strField);
    }

    @Test
    public void testAvroConverter() throws Exception {
        Schema schema = SchemaBuilder.struct()
            .field("intField", SchemaBuilder.int32().optional())
            .field("strField", SchemaBuilder.string().optional())
            .build();
        Struct struct = new Struct(schema)
            .put("intField", 32)
            .put("strField", "value-32");
        SchemaAndValue data = new SchemaAndValue(
            schema,
            struct
        );

        org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(data.schema());
        log.info("Avro Schema : {}", avroSchema);

        // write to json bytes
        jsonSchema.setAvroSchema(deserializer.getConverter(), null,  avroSchema);
        byte[] jsonBytes = jsonSchema.encode(data);

        // deserialize json bytes to `SchemaAndValue`
        SchemaAndValue value = deserializer.deserialize("test-topic", jsonBytes);
        assertEquals(
            "\nExpect schema : " + avroData.fromConnectSchema(schema) + "\n"
                + "Actual schema : " + avroData.fromConnectSchema(value.schema()),
            avroData.fromConnectSchema(schema),
            avroData.fromConnectSchema(value.schema())
        );
        Struct actualStruct = (Struct) value.value();
        assertEquals(
            32, actualStruct.getInt32("intField").intValue());
        assertEquals(
            "value-32", actualStruct.getString("strField"));

        // encode connect data in avro format
        jsonSchema.setAvroSchema(
            null, avroData, avroSchema
        );
        byte[] avroBytes = jsonSchema.encode(value);
        org.apache.pulsar.client.api.Schema<GenericRecord> genericSchema =
            org.apache.pulsar.client.api.Schema.generic(SchemaInfo.builder()
                .name("test")
                .type(SchemaType.AVRO)
                .schema(avroSchema.toString().getBytes(UTF_8))
                .properties(Collections.emptyMap())
                .build());
        GenericRecord avroRecord = genericSchema.decode(avroBytes);
        assertEquals(32, avroRecord.getField("intField"));
        assertEquals("value-32", avroRecord.getField("strField"));
    }

}
