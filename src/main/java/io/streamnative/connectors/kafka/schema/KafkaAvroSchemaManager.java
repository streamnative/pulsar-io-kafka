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


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

/**
 * A manager to manage Kafka schemas and convert Kafka schema to Pulsar schema.
 */
@Slf4j
public class KafkaAvroSchemaManager implements AutoCloseable {

    private final SchemaRegistryClient client;
    private final ConcurrentMap<Integer, Schema> kafkaSchemas = new ConcurrentHashMap<>();

    public KafkaAvroSchemaManager(SchemaRegistryClient client,
                                  Map<String, ?> props) {

        if (null == client) {
            this.client = configureSchemaRegistryClient(deserializerConfig(props));
        } else {
            this.client = client;
        }
    }

    public Schema getKafkaSchema(int id) throws SerializationException {
        return kafkaSchemas.computeIfAbsent(id, schemaId -> {
            try {
                return client.getById(id);
            } catch (RuntimeException | IOException | RestClientException e) {
                throw new SerializationException("Error retrieving Avro schema from Kafka schema registry for id "
                    + schemaId, e);
            }
        });
    }

    @Override
    public void close() {
    }

    protected static KafkaAvroSchemaManagerConfig deserializerConfig(Map<String, ?> props) {
        return new KafkaAvroSchemaManagerConfig(props);
    }

    protected static CachedSchemaRegistryClient configureSchemaRegistryClient(KafkaAvroSchemaManagerConfig config) {
        List<String> urls = config.getSchemaRegistryUrls();
        int maxSchemaObject = config.getMaxSchemasPerSubject();
        Map<String, Object> originals = config.originalsWithPrefix("");
        return new CachedSchemaRegistryClient(urls, maxSchemaObject, originals);
    }

}
