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

import static io.streamnative.connectors.kafka.AvroTestSchemas.USER_SCHEMA;
import static org.junit.Assert.assertEquals;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

/**
 * Unit test {@link KafkaAvroSchemaManager}.
 */
public class KafkaAvroSchemaManagerTest {

    private final String topic;
    private final SchemaRegistryClient schemaRegistry;
    private final KafkaAvroSchemaManager schemaManager;

    public KafkaAvroSchemaManagerTest() {
        this.topic = "test";
        this.schemaRegistry = new MockSchemaRegistryClient();

        HashMap<String, String> schemaManagerConfig = new HashMap<>();
        // Intentionally invalid schema registry URL to satisfy the config class's requirement that
        // it be set.
        schemaManagerConfig.put(KafkaAvroSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");

        this.schemaManager = new KafkaAvroSchemaManager(
            schemaRegistry,
            schemaManagerConfig
        );
    }

    @Test
    public void testGetKafkaSchemaSuccess() throws Exception {
        int id = schemaRegistry.register(
            topic,
            USER_SCHEMA
        );

        Schema schema = schemaManager.getKafkaSchema(id);
        assertEquals(USER_SCHEMA, schema);
    }

    @Test(expected = SerializationException.class)
    public void testGetKafkaSchemaNotFound() {
        schemaManager.getKafkaSchema(0x12345);
    }

}
