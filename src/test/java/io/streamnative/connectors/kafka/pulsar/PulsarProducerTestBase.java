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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import io.streamnative.connectors.kafka.serde.KafkaSchemaAndBytes;
import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import java.nio.ByteBuffer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
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
    public static class User {
        String name;
        int age;
    }

    /**
     * Student class.
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student {
        String name;
        int age;
        @AvroDefault(value = "\"0\"")
        Double gpa;
    }

    /**
     * A decoder to decode a value from a given partition to an object.
     */
    public interface AvroValueDecoder {

        Object decode(int partition, int sequence, byte[] data);

    }

    /**
     * A verifier to verify a value from a given partition.
     */
    public interface AvroValueVerifier<V> {

        void verify(int partition, int sequence, V value);

    }

    public static final AvroSchema<User> PULSAR_USER_SCHEMA = AvroSchema.of(
        SchemaDefinition.<User>builder()
            .withPojo(User.class)
            .withAlwaysAllowNull(false)
            .withSupportSchemaVersioning(true)
           .build());
    public static final String AVRO_USER_SCHEMA_DEF = PULSAR_USER_SCHEMA.getAvroSchema().toString();
    public static final Schema AVRO_USER_SCHEMA = new Schema.Parser().parse(AVRO_USER_SCHEMA_DEF);

    public static final AvroSchema<Student> PULSAR_STUDENT_SCHEMA = AvroSchema.of(
        SchemaDefinition.<Student>builder()
            .withPojo(Student.class)
            .withAlwaysAllowNull(false)
            .withSupportSchemaVersioning(true)
            .build());
    public static final String AVRO_STUDENT_SCHEMA_DEF = PULSAR_STUDENT_SCHEMA.getAvroSchema().toString();
    public static final Schema AVRO_STUDENT_SCHEMA = new Schema.Parser().parse(AVRO_STUDENT_SCHEMA_DEF);

    public static final Generator<byte[]> KEY_GENERATOR =
        (partition, sequence) -> String.format("key-%d-%d", partition, sequence).getBytes(UTF_8);

    public static final Generator<byte[]> BYTES_GENERATOR =
        (partition, sequence) -> String.format("value-%d-%d", partition, sequence).getBytes(UTF_8);
    public static final Generator<Boolean> BOOLEAN_GENERATOR =
        (partition, sequence) -> (partition * 100 + sequence) % 2 == 0 ? true : false;
    public static final Generator<Byte> BYTE_GENERATOR =
        (partition, sequence) -> (byte) (partition * 100 + sequence);
    public static final Generator<Short> SHORT_GENERATOR =
        (partition, sequence) -> (short) (partition * 100 + sequence);
    public static final Generator<Integer> INTEGER_GENERATOR =
        (partition, sequence) -> partition * 100 + sequence;
    public static final Generator<Long> LONG_GENERATOR =
        (partition, sequence) -> partition * 100L + sequence;
    public static final Generator<Float> FLOAT_GENERATOR =
        (partition, sequence) -> partition * 100.0f + sequence;
    public static final Generator<Double> DOUBLE_GENERATOR =
        (partition, sequence) -> partition * 100.0d + sequence;
    public static final Generator<String> STRING_GENERATOR =
        (partition, sequence) -> String.format("value-%d-%d", partition, sequence);
    public static final Generator<User> USER_GENERATOR =
        (partition, sequence) -> {
            if (sequence % 2 == 0) {
                return new User("user-" + partition + "-" + sequence, 10 * sequence);
            } else {
                return new User(
                    "student-" + partition + "-" + sequence,
                    10 * sequence
                );
            }
        };
    public static final Generator<Student> STUDENT_GENERATOR =
        (partition, sequence) -> {
            if (sequence % 2 == 0) {
                return new Student(
                    "user-" + partition + "-" + sequence,
                    10 * sequence,
                    (double) 0
                );
            } else {
                return new Student(
                    "student-" + partition + "-" + sequence,
                    10 * sequence,
                    1.0d * sequence
                );
            }
        };
    public static final Generator<Object> AVRO_OBJECT_GENERATOR =
        (partition, sequence) -> {
            if (sequence % 2 == 0) {
                return USER_GENERATOR.apply(partition, sequence);
            } else {
                return STUDENT_GENERATOR.apply(partition, sequence);
            }
        };
    public static final Generator<Object> AVRO_GENERIC_RECORD_GENERATOR =
        (partition, sequence) -> {
            GenericRecord record;
            if (sequence % 2 == 0) {
                User user = USER_GENERATOR.apply(partition, sequence);
                record = new Record(AVRO_USER_SCHEMA);
                record.put("name", user.name);
                record.put("age", user.age);
            } else {
                Student student = STUDENT_GENERATOR.apply(partition, sequence);
                record = new Record(AVRO_STUDENT_SCHEMA);
                record.put("name", student.name);
                record.put("age", student.age);
                record.put("gpa", student.gpa);
            }
            return record;
        };
    public static final AvroValueDecoder AVRO_VALUE_DECODER =
        (partition, sequence, bytes) -> {
            if (sequence % 2 == 0) {
                return PULSAR_USER_SCHEMA.decode(bytes);
            } else {
                return PULSAR_STUDENT_SCHEMA.decode(bytes);
            }
        };
    public static final AvroValueVerifier<org.apache.pulsar.client.api.schema.GenericRecord> AVRO_VALUE_VERIFIER =
        (partition, sequence, record) -> {
            if (sequence % 2 == 0) {
                assertEquals(
                    "user-" + partition + "-" + sequence,
                    record.getField("name")
                );
                assertEquals(
                    10 * sequence,
                    record.getField("age")
                );
            } else {
                assertEquals(
                    "student-" + partition + "-" + sequence,
                    record.getField("name")
                );
                assertEquals(
                    10 * sequence,
                    record.getField("age")
                );
                assertEquals(
                    1.0d * sequence,
                    record.getField("gpa")
                );
            }
        };

    /**
     * A generator to generate users and students.
     */
    @RequiredArgsConstructor
    public static class MultiVersionGenerator implements Generator<KafkaSchemaAndBytes> {

        private final int userSchemaId;
        private final int studentSchemaId;

        @Override
        public KafkaSchemaAndBytes apply(int partition, int sequence) {
            if (sequence % 2 == 0) {
                User user = new User(
                    "user-" + partition + "-" + sequence,
                    10 * sequence
                );
                byte[] userData = PULSAR_USER_SCHEMA.encode(user);
                return new KafkaSchemaAndBytes(
                    userSchemaId,
                    ByteBuffer.wrap(userData)
                );
            } else {
                Student student = new Student(
                    "student-" + partition + "-" + sequence,
                    10 * sequence,
                    1.0d * sequence
                );
                byte[] studentData = PULSAR_STUDENT_SCHEMA.encode(student);
                return new KafkaSchemaAndBytes(
                    studentSchemaId,
                    ByteBuffer.wrap(studentData)
                );
            }
        }

    }

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

    public static void verifyMultiVersionData(int partition,
                                              int sequence,
                                              byte[] data,
                                              org.apache.pulsar.client.api.Schema<?> schema,
                                        Generator<?> generator) {
        if (generator instanceof MultiVersionGenerator) {
            // this is a multi version generator
            if (sequence % 2 == 0) {
                User user = PULSAR_USER_SCHEMA.decode(data);
                User expectedUser = new User(
                    "user-" + partition + "-" + sequence,
                    10 * sequence
                );
                assertEquals(
                    expectedUser,
                    user
                );
            } else {
                Student student = PULSAR_STUDENT_SCHEMA.decode(data);
                Student expectedStudent = new Student(
                    "student-" + partition + "-" + sequence,
                    10 * sequence,
                    1.0d * sequence
                );
                assertEquals(
                    expectedStudent,
                    student
                );
            }
        } else {
            assertValueEquals(
                partition, sequence,
                generator,
                schema.decode(data)
            );
        }

    }

    public static void assertValueEquals(int partition, int sequence, Generator<?> generator, Object actualValue) {
        if (null == generator) {
            assertNull(actualValue);
        } else {
            if (actualValue instanceof byte[]) {
                assertArrayEquals(
                    (byte[]) generator.apply(partition, sequence),
                    (byte[]) actualValue
                );
            } else {
                assertEquals(
                    generator.apply(partition, sequence),
                    actualValue
                );
            }
        }
    }


}
