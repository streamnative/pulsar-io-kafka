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

import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.serializers.NonRecordContainer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * A schema to encoding kafka json value.
 */
public class KafkaJsonSchema implements Schema<SchemaAndValue> {

    private AvroData avroData = null;
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private Converter jsonConverter = null;
    private SchemaInfo schemaInfo = null;
    private org.apache.avro.Schema avroSchema = null;

    public void setAvroSchema(JsonConverter converter,
                              AvroData avroData,
                              org.apache.avro.Schema schema) {
        this.jsonConverter = converter;
        this.avroData = avroData;
        this.avroSchema = schema;
        this.schemaInfo = SchemaInfo.builder()
            .name(jsonConverter != null ? "KafkaJson" : "KafkaAvro")
            .type(jsonConverter != null ? SchemaType.JSON : SchemaType.AVRO)
            .properties(Collections.emptyMap())
            .schema(schema.toString().getBytes(UTF_8))
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    public byte[] encode(SchemaAndValue connectData) {
        if (null != jsonConverter) {
            return jsonConverter.fromConnectData("", connectData.schema(), connectData.value());
        } else if (null != avroData) {
            Object object = avroData.fromConnectData(
                connectData.schema(),
                connectData.value()
            );

            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                if (object instanceof byte[]) {
                    out.write(((byte[]) object));
                } else {
                    BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, null);
                    Object value =
                        object instanceof NonRecordContainer ? ((NonRecordContainer) object).getValue() : object;
                    Object writer;
                    if (value instanceof SpecificRecord) {
                        writer = new SpecificDatumWriter(avroSchema);
                    } else {
                        writer = new GenericDatumWriter(avroSchema);
                    }

                    ((DatumWriter) writer).write(value, encoder);
                    encoder.flush();
                }

                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            } catch (RuntimeException | IOException e) {
                throw new SchemaSerializationException(e);
            }
        } else {
            throw new SchemaSerializationException("Schema is not initialized to serialize value");
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return schemaInfo;
    }
}
