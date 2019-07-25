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
package io.streamnative.connectors.kafka.pulsar;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroDefault;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;

/**
 * A system test base for running pulsar producer tests.
 */
public class PulsarProducerTestBase extends PulsarServiceSystemTestCase {

    /**
     * User class.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class User {
        String name;
        int age;
    }

    /**
     * Student class.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class Student {
        String name;
        int age;
        @AvroDefault(value = "\"0\"")
        Double gpa;
    }

    protected static final AvroSchema<User> PULSAR_USER_SCHEMA = AvroSchema.of(
        SchemaDefinition.<User>builder()
            .withPojo(User.class)
            .withAlwaysAllowNull(false)
            .withSupportSchemaVersioning(true)
           .build());
    protected static final String AVRO_USER_SCHEMA_DEF = PULSAR_USER_SCHEMA.getAvroSchema().toString();
    protected static final Schema AVRO_USER_SCHEMA = new Schema.Parser().parse(AVRO_USER_SCHEMA_DEF);

    protected static final AvroSchema<Student> PULSAR_STUDENT_SCHEMA = AvroSchema.of(
        SchemaDefinition.<Student>builder()
            .withPojo(Student.class)
            .withAlwaysAllowNull(false)
            .withSupportSchemaVersioning(true)
            .build());
    protected static final String AVRO_STUDENT_SCHEMA_DEF = PULSAR_STUDENT_SCHEMA.getAvroSchema().toString();
    protected static final Schema AVRO_STUDENT_SCHEMA = new Schema.Parser().parse(AVRO_STUDENT_SCHEMA_DEF);

    protected static final Generator<byte[]> KEY_GENERATOR =
        (partition, sequence) -> String.format("key-%d-%d", partition, sequence).getBytes(UTF_8);

    protected static final Generator<byte[]> BYTES_GENERATOR =
        (partition, sequence) -> String.format("value-%d-%d", partition, sequence).getBytes(UTF_8);
    protected static final Generator<Boolean> BOOLEAN_GENERATOR =
        (partition, sequence) -> (partition * 100 + sequence) % 2 == 0 ? true : false;
    protected static final Generator<Byte> BYTE_GENERATOR =
        (partition, sequence) -> (byte) (partition * 100 + sequence);
    protected static final Generator<Short> SHORT_GENERATOR =
        (partition, sequence) -> (short) (partition * 100 + sequence);
    protected static final Generator<Integer> INTEGER_GENERATOR =
        (partition, sequence) -> partition * 100 + sequence;
    protected static final Generator<Long> LONG_GENERATOR =
        (partition, sequence) -> partition * 100L + sequence;
    protected static final Generator<Float> FLOAT_GENERATOR =
        (partition, sequence) -> partition * 100.0f + sequence;
    protected static final Generator<Double> DOUBLE_GENERATOR =
        (partition, sequence) -> partition * 100.0d + sequence;
    protected static final Generator<String> STRING_GENERATOR =
        (partition, sequence) -> String.format("value-%d-%d", partition, sequence);
    protected static final Generator<User> USER_GENERATOR =
        (partition, sequence) -> new User("user-" + partition, 10 * sequence);

    public PulsarProducerTestBase(PulsarService service) {
        super(service);
    }

    protected static <K, V> ConsumerRecord<Object, Object> newKafkaRecord(
        String topic, int partition, int i,
        Generator<K> keyGenerator,
        Generator<V> valueGenerator
    ) {
        return new ConsumerRecord<>(
            topic,
            partition,
            i * 1000L,
            (i + 1) * 10000L,
            TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_CHECKSUM,
            ConsumerRecord.NULL_SIZE,
            ConsumerRecord.NULL_SIZE,
            null == keyGenerator ? null : keyGenerator.apply(partition, i),
            null == valueGenerator ? null : valueGenerator.apply(partition, i)
        );
    }



}
