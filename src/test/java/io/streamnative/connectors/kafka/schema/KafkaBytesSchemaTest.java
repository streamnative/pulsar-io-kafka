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
import static org.junit.Assert.assertSame;
import org.apache.kafka.common.utils.Bytes;
import org.apache.pulsar.client.api.Schema;
import org.junit.Test;

/**
 * Unit test {@link KafkaBytesSchema}.
 */
public class KafkaBytesSchemaTest {

    private final KafkaBytesSchema schema;

    public KafkaBytesSchemaTest() {
        this.schema = KafkaBytesSchema.of();
    }

    @Test
    public void testEncodeDecode() {
        final String testString = "test-string-" + System.currentTimeMillis();
        final byte[] testData = testString.getBytes(UTF_8);
        Bytes bytes = schema.decode(testData);
        assertSame(testData, bytes.get());
        byte[] encodedData = schema.encode(bytes);
        assertSame(testData, encodedData);
    }

    @Test
    public void testGetSchemaInfo() {
        assertEquals(
            Schema.BYTES.getSchemaInfo(),
            schema.getSchemaInfo()
        );
    }

}
