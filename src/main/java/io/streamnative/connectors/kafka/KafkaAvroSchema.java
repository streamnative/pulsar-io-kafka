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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Read from Kafka (and schema registry) and write to Pulsar.
 */
public class KafkaAvroSchema implements Schema<KafkaSchemaAndBytes> {

    private org.apache.avro.Schema avroSchema = null;
    private SchemaInfo schemaInfo = null;

    public KafkaAvroSchema() {
    }

    public void setAvroSchema(org.apache.avro.Schema schema) {
        this.avroSchema = schema;
        this.schemaInfo = SchemaInfo.builder()
            .name("KafkaAvro")
            .type(SchemaType.AVRO)
            .properties(Collections.emptyMap())
            .schema(schema.toString().getBytes(UTF_8))
            .build();
    }

    @Override
    public byte[] encode(KafkaSchemaAndBytes ksBytes) {
        ByteBuffer bb = ksBytes.getData();
        byte[] data = new byte[bb.remaining()];
        bb.get(data);
        return data;
    }

    @Override
    public KafkaSchemaAndBytes decode(byte[] bytes) {
        throw new UnsupportedOperationException("Decoding is not supported in this schema implementation.");
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
