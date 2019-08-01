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

import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializer to deserialize avro types from Kafka messages.
 */
public class KafkaSchemaAndBytesDeserializer implements Deserializer<KafkaSchemaAndBytes> {

    public static KafkaSchemaAndBytesDeserializer of() {
        return INSTANCE;
    }

    private static final KafkaSchemaAndBytesDeserializer INSTANCE = new KafkaSchemaAndBytesDeserializer();

    @Override
    public KafkaSchemaAndBytes deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        } else {
            ByteBuffer buffer = getByteBuffer(data);
            int kafkaSchemaId = buffer.getInt();

            return new KafkaSchemaAndBytes(
                kafkaSchemaId,
                buffer
            );
        }
    }

    private static ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }
}
