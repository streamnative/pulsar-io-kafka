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

import org.apache.kafka.common.utils.Bytes;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A schema for Kafka {@link Bytes}.
 */
public class KafkaBytesSchema implements Schema<Bytes> {

    public static KafkaBytesSchema of() {
        return INSTANCE;
    }

    private static final KafkaBytesSchema INSTANCE = new KafkaBytesSchema();

    private KafkaBytesSchema() {}

    @Override
    public byte[] encode(Bytes bytes) {
        return bytes.get();
    }

    @Override
    public Bytes decode(byte[] bytes) {
        return Bytes.wrap(bytes);
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return Schema.BYTES.getSchemaInfo();
    }
}
