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

import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.shade.org.apache.avro.generic.GenericDatumReader;
import org.apache.pulsar.shade.org.apache.avro.io.Decoder;
import org.apache.pulsar.shade.org.apache.avro.io.DecoderFactory;
import org.junit.Test;

/**
 * Schema
 */
public class SchemaTest {

    private static final String PULSAR_SCHEMA_1 = "{\n" +
        "    \"type\": \"record\",\n" +
        "    \"name\": \"User\",\n" +
        "    \"namespace\": \"io.streamnative.connectors.kafka.example\",\n" +
        "    \"fields\": [\n" +
        "      {\n" +
        "        \"name\": \"name\",\n" +
        "        \"type\": [\n" +
        "          \"string\",\n" +
        "          \"null\"\n" +
        "        ]\n" +
        "      },\n" +
        "      {\n" +
        "        \"name\": \"age\",\n" +
        "        \"type\": [\n" +
        "          \"string\",\n" +
        "          \"null\"\n" +
        "        ]\n" +
        "      },\n" +
        "      {\n" +
        "        \"name\": \"gpa\",\n" +
        "        \"type\": [\n" +
        "          \"string\",\n" +
        "          \"null\"\n" +
        "        ]\n" +
        "      }\n" +
        "    ]\n" +
        "  }";

    private static final String PULSAR_SCHEMA_2 = "{\n" +
        "    \"type\": \"record\",\n" +
        "    \"name\": \"User\",\n" +
        "    \"namespace\": \"io.streamnative.connectors.kafka.example\",\n" +
        "    \"fields\": [\n" +
        "      {\n" +
        "        \"name\": \"name\",\n" +
        "        \"type\": [\n" +
        "          \"null\",\n" +
        "          \"string\"\n" +
        "        ]\n" +
        "      },\n" +
        "      {\n" +
        "        \"name\": \"age\",\n" +
        "        \"type\": [\n" +
        "          \"null\",\n" +
        "          \"string\"\n" +
        "        ]\n" +
        "      },\n" +
        "      {\n" +
        "        \"name\": \"gpa\",\n" +
        "        \"type\": [\n" +
        "          \"null\",\n" +
        "          \"string\"\n" +
        "        ]\n" +
        "      }\n" +
        "    ]\n" +
        "  }";

    private static final String PRESTO_SCHEMA = "{\"type\":\"record\",\"name\":\"User\",\"namespace\":\"io.streamnative.connectors.kafka.example\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"age\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"gpa\",\"type\":[\"null\",\"string\"],\"default\":null}]}";

    @SuppressWarnings("unchecked")
    @Test
    public void testSchemaWriteRead() throws Exception {
        Schema pulsarSchema1 = new Schema.Parser().parse(PULSAR_SCHEMA_1);
        Schema pulsarSchema2 = new Schema.Parser().parse(PULSAR_SCHEMA_2);
        org.apache.pulsar.shade.org.apache.avro.Schema prestoSchema1 =
            new org.apache.pulsar.shade.org.apache.avro.Schema.Parser().parse(PULSAR_SCHEMA_1);
        org.apache.pulsar.shade.org.apache.avro.Schema prestoSchema2 =
            new org.apache.pulsar.shade.org.apache.avro.Schema.Parser().parse(PRESTO_SCHEMA);

        System.out.println("Pulsar schema 1 : " + pulsarSchema1.toString());
        System.out.println("Pulsar schema 2 : " + pulsarSchema2.toString());
        System.out.println("Presto schema 1 : " + prestoSchema1.toString());
        System.out.println("Presto schema 2 : " + prestoSchema2.toString());

        BinaryEncoder encoder = null;
        GenericDatumWriter<GenericRecord> writer1 = new GenericDatumWriter<>(pulsarSchema1);
        ByteArrayOutputStream byteArrayOutputStream1 = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream1, encoder);

        GenericRecord record = new GenericRecordBuilder(pulsarSchema1)
            .set("name", "test")
            .set("age", "1000")
            .set("gpa", "1")
            .build();

        writer1.write(record, encoder);
        encoder.flush();
        byte[] data1 = byteArrayOutputStream1.toByteArray();

        GenericDatumWriter<GenericRecord> writer2 = new GenericDatumWriter<>(pulsarSchema1);
        ByteArrayOutputStream byteArrayOutputStream2 = new ByteArrayOutputStream();
        encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream2, encoder);
        writer2.write(record, encoder);
        encoder.flush();
        byte[] data2 = byteArrayOutputStream2.toByteArray();

        // read
        System.out.println("Using presto schema v2 to decode v1 data failed");
        GenericDatumReader reader1 = new GenericDatumReader<>(prestoSchema2);
        Decoder decoder = DecoderFactory.get().binaryDecoder(data1, null);
        try {
            org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord prestoRecord =
                (org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord) reader1.read(null, decoder);
        } catch (ArrayIndexOutOfBoundsException e) {
            // expected
            e.printStackTrace();
        }

        System.out.println("Using presto v1 and v2 schema to decode v1 data should succeed : ");
        GenericDatumReader reader2 = new GenericDatumReader<>(prestoSchema1, prestoSchema2);
        decoder = DecoderFactory.get().binaryDecoder(data1, null);
        org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord prestoRecord =
            (org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord) reader2.read(null, decoder);

        System.out.println("Name:\t" + prestoRecord.get("name"));
        System.out.println("Age:\t" + prestoRecord.get("age"));
        System.out.println("Gpa:\t" + prestoRecord.get("gpa"));

        // read
        System.out.println("Using presto schema v1 to decode v2 data succeed");
        GenericDatumReader reader3 = new GenericDatumReader<>(prestoSchema1);
        decoder = DecoderFactory.get().binaryDecoder(data2, null);
        org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord prestoRecord3 =
            (org.apache.pulsar.shade.org.apache.avro.generic.GenericRecord) reader3.read(null, decoder);

        System.out.println("Name:\t" + prestoRecord3.get("name"));
        System.out.println("Age:\t" + prestoRecord3.get("age"));
        System.out.println("Gpa:\t" + prestoRecord3.get("gpa"));
    }

}
