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

import static io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.AVRO_USER_SCHEMA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import io.confluent.connect.avro.AvroData;
import io.streamnative.connectors.kafka.KafkaSourceConfig.ConverterType;
import io.streamnative.connectors.kafka.pulsar.PulsarProducerTestBase.User;
import java.util.Collections;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Before;
import org.junit.Test;

/**
 * Test Json Schema.
 */
@Slf4j
public class KafkaJsonSchemaTest {

    private final KafkaJsonSchema jsonSchema;
    private final AvroData avroData;
    private final Schema<User> userJsonSchema = Schema.JSON(
        SchemaDefinition.<User>builder()
            .withPojo(User.class)
            .withAlwaysAllowNull(false)
            .withSupportSchemaVersioning(true)
            .build());

    public KafkaJsonSchemaTest() {
        jsonSchema = new KafkaJsonSchema();
        avroData = new AvroData(1000);
    }

    @Before
    public void setup() {
    }

    @Test
    public void testJsonConverter() throws Exception {
        jsonSchema.setAvroSchema(
            false,
            avroData,
            AVRO_USER_SCHEMA,
            KafkaJsonSchemaManager.getConverter(ConverterType.JSON, false)
        );

        User user = new User("user-1", 100);
        byte[] data = userJsonSchema.encode(user);
        byte[] encodedData = jsonSchema.encode(data);
        assertSame(data, encodedData);
    }

    @Test
    public void testAvroConverter() throws Exception {
        jsonSchema.setAvroSchema(
            false,
            avroData,
            AVRO_USER_SCHEMA,
            KafkaJsonSchemaManager.getConverter(ConverterType.AVRO, false)
        );

        log.info("Initialized with avro schema {}", AVRO_USER_SCHEMA);

        User user = new User("user-1", 100);
        byte[] jsonData = userJsonSchema.encode(user);

        byte[] avroData = jsonSchema.encode(jsonData);

        Schema<GenericRecord> userAvroSchema = Schema.generic(
            SchemaInfo.builder()
                .name("")
                .properties(Collections.emptyMap())
                .type(SchemaType.AVRO)
                .schema(userJsonSchema.getSchemaInfo().getSchema())
                .build()
        );
        GenericRecord userRecord = userAvroSchema.decode(avroData);
        assertEquals(user.getName(), userRecord.getField("name"));
        assertEquals(user.getAge(), userRecord.getField("age"));

    }

}
