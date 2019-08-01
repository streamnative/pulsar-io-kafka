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

import io.confluent.connect.avro.AvroConverter;
import io.confluent.connect.avro.AvroData;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.streamnative.connectors.kafka.KafkaSourceConfig;
import io.streamnative.connectors.kafka.KafkaSourceConfig.ConverterType;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.apache.avro.Schema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.Converter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A manager to manage Kafka json schema.
 */
@Getter
@Accessors(fluent = true)
public class KafkaJsonSchemaManager implements AutoCloseable {

    private final AvroData avroData;
    private final Schema keySchema;
    private final Converter keyConverter;
    private final Schema valueSchema;
    private final Converter valueConverter;

    public KafkaJsonSchemaManager(PulsarAdmin admin,
                                  KafkaSourceConfig config) throws Exception {
        this.avroData = new AvroData(1000);
        switch (config.kafka().schema().json_schema_provider()) {
            case CONFIG:
                Schema keySchema = parseSchema(config.kafka().schema().key_schema());
                Schema valueSchema = parseSchema(config.kafka().schema().value_schema());
                if (keySchema == null && valueSchema == null) {
                    throw new IllegalArgumentException("Neither key schema nor value schema is defined"
                        + " when using CONFIG schema provider.");
                }
                this.keySchema = keySchema;
                this.valueSchema = valueSchema;
                this.keyConverter = getConverter(
                    config.kafka().schema().key_converter(), true
                );
                this.valueConverter = getConverter(
                    config.kafka().schema().value_converter(), false
                );
                break;
            case PULSAR:
                if (config.kafka().schema().key_converter() != null
                    || config.kafka().schema().value_converter() != null) {
                    throw new IllegalArgumentException("Key or value converter is define when using"
                        + " PULSAR schema provider. Please disable converters.");
                }

                try {
                    SchemaInfo schemaInfo = admin.schemas().getSchemaInfo(
                        config.pulsar().topic()
                    );
                    switch (schemaInfo.getType()) {
                        case AVRO:
                            this.keyConverter = getConverter(ConverterType.AVRO, true);
                            this.keySchema = null;
                            this.valueConverter = getConverter(ConverterType.AVRO, false);
                            this.valueSchema = parseSchema(schemaInfo.getSchemaDefinition());
                            break;
                        case JSON:
                            this.keyConverter = getConverter(ConverterType.JSON, true);
                            this.keySchema = null;
                            this.valueConverter = getConverter(ConverterType.JSON, false);
                            this.valueSchema = parseSchema(schemaInfo.getSchemaDefinition());
                            break;
                        /** {@link https://github.com/apache/pulsar/issues/4840}
                        case KEY_VALUE:
                            KeyValue<SchemaInfo, SchemaInfo> kvSchemaInfo =
                                KeyValueSchemaInfo.decodeKeyValueSchemaInfo(schemaInfo);
                            KeyValue<Schema, Converter> keySchemaConverter = getSchemaInfo(
                                kvSchemaInfo.getKey(), true
                            );
                            this.keySchema = keySchemaConverter.getKey();
                            this.keyConverter = keySchemaConverter.getValue();
                            KeyValue<Schema, Converter> valueSchemaConverter = getSchemaInfo(
                                kvSchemaInfo.getValue(), false
                            );
                            this.valueSchema = valueSchemaConverter.getKey();
                            this.valueConverter = valueSchemaConverter.getValue();
                            break;
                            **/
                        default:
                            throw new IllegalArgumentException("The schema of the Pulsar topic is"
                                + " not AVRO/JSON/KEY_VALUE");
                    }
                } catch (PulsarAdminException pae) {
                    throw new Exception(
                        "Failed to retrieve schema info from Pulsar topic when using PULSAR schema provider",
                        pae
                    );
                }
                break;
            default:
                throw new UnsupportedOperationException("Current "
                    + config.kafka().schema().json_schema_provider() + " schema provider is not supported");
        }
    }

    public static KeyValue<Schema, Converter> getSchemaInfo(SchemaInfo si, boolean isKey) {
        switch (si.getType()) {
            case AVRO:
                return new KeyValue<>(
                    parseSchema(si.getSchemaDefinition()),
                    getConverter(ConverterType.AVRO, isKey)
                );
            case JSON:
                return new KeyValue<>(
                    parseSchema(si.getSchemaDefinition()),
                    getConverter(ConverterType.JSON, isKey)
                );
            default:
                return new KeyValue<>(
                    null,
                    null
                );
        }
    }

    public static Converter getConverter(ConverterType ct, boolean isKey) {
        if (null == ct) {
            ct = ConverterType.AVRO;
        }
        Converter converter;
        switch (ct) {
            case AVRO:
                // we don't need to connect to schema registry
                converter = new AvroConverter(new MockSchemaRegistryClient());
                break;
            case JSON:
                converter = new JsonConverter();
                break;
            default:
                throw new IllegalArgumentException("Unknown converter : " + ct);
        }

        Map<String, Object> config = new HashMap<>();
        config.put(
            KafkaAvroSchemaManagerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            "mock");
        converter.configure(config, isKey);
        return converter;
    }

    private static Schema parseSchema(String schemaDef) {
        if (null == schemaDef) {
            return null;
        }
        return new Schema.Parser().parse(schemaDef);
    }

    @Override
    public void close() {
    }
}
