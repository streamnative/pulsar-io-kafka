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

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.storage.ConverterType;

/**
 * Deserializer to deserialize json value from Kafka messages.
 */
public class KafkaConnectDataDeserializer implements Deserializer<SchemaAndValue> {

    public static KafkaConnectDataDeserializer of() {
        return INSTANCE;
    }

    private static final KafkaConnectDataDeserializer INSTANCE =
        new KafkaConnectDataDeserializer();

    @Getter
    private final JsonConverter converter = new JsonConverter();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> newConfig = new HashMap<>();
        newConfig.putAll(configs);
        ConverterType type;
        if (isKey) {
            type = ConverterType.KEY;
        } else {
            type = ConverterType.VALUE;
        }
        newConfig.put("converter.type", type.getName());
        newConfig.put("schemas.enable", true);
        converter.configure(newConfig);
    }

    @Override
    public SchemaAndValue deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        } else {
            return converter.toConnectData(topic, data);
        }
    }
}
