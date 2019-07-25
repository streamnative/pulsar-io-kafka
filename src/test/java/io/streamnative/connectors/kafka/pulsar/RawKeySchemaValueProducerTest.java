package io.streamnative.connectors.kafka.pulsar;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import io.streamnative.connectors.kafka.KafkaMessageRouter;
import io.streamnative.tests.common.framework.SystemTestRunner;
import io.streamnative.tests.common.framework.SystemTestRunner.TestSuiteClass;
import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.HashingScheme;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.KeyValue;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A system test to test producer with raw key and schema value.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(PulsarProducerTestSuite.class)
@Slf4j
public class RawKeySchemaValueProducerTest extends PulsarServiceSystemTestCase {

    public RawKeySchemaValueProducerTest(PulsarService service) {
        super(service);
    }

    private <V> ConsumerRecord<Object, Object> newKafkaRecord(String topic,
                                                              int partition,
                                                              int i,
                                                              Function<KeyValue<Integer, Integer>, V> valueGenerator) {
        return new ConsumerRecord<>(
            topic,
            partition,
            i * 1000L,
            (i + 1) * 10000L,
            TimestampType.CREATE_TIME,
            ConsumerRecord.NULL_CHECKSUM,
            ConsumerRecord.NULL_SIZE,
            ConsumerRecord.NULL_SIZE,
            String.format("key-%d-%d", partition, i).getBytes(UTF_8),
            valueGenerator.apply(new KeyValue<>(partition, i))
        );
    }

    private <V> void verifyPulsarMessage(Message<V> pulsarMessage,
                                         int partition,
                                         int i,
                                         Function<KeyValue<Integer, Integer>, V> valueGenerator) {
        assertEquals(i * 1000L, pulsarMessage.getSequenceId());
        assertEquals((i + 1) * 10000L, pulsarMessage.getEventTime());
        assertArrayEquals(
            String.format("key-%d-%d", partition, i).getBytes(UTF_8),
            pulsarMessage.getKeyBytes()
        );
        if (pulsarMessage.getValue() instanceof byte[]) {
            assertArrayEquals(
                (byte[]) valueGenerator.apply(new KeyValue<>(partition, i)),
                (byte[]) pulsarMessage.getValue()
            );
        } else {
            assertEquals(
                valueGenerator.apply(new KeyValue<>(partition, i)),
                pulsarMessage.getValue()
            );
        }
    }

    @Test
    public void testBytesSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.BYTES,
            kv -> String.format("value-%d-%d", kv.getKey(), kv.getValue()).getBytes(UTF_8));
    }

    @Test
    public void testBooleanSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.BOOL,
            kv -> (kv.getKey() * 100 + kv.getValue()) % 2 == 0 ? true : false);
    }

    @Test
    public void testInt8Schema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT8,
            kv -> (byte) (kv.getKey() * 100 + kv.getValue()));
    }

    @Test
    public void testInt16Schema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT16,
            kv -> (short) (kv.getKey() * 100 + kv.getValue()));
    }

    @Test
    public void testInt32chema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT32,
            kv -> kv.getKey() * 100 + kv.getValue());
    }

    @Test
    public void testInt64chema() throws Exception {
        testSendMessages(
            10, 10, Schema.INT64,
            kv -> kv.getKey() * 100L + kv.getValue());
    }

    @Test
    public void testFloatchema() throws Exception {
        testSendMessages(
            10, 10, Schema.FLOAT,
            kv -> kv.getKey() * 100.0f + kv.getValue());
    }

    @Test
    public void testDoublechema() throws Exception {
        testSendMessages(
            10, 10, Schema.DOUBLE,
            kv -> kv.getKey() * 100.0d + kv.getValue());
    }

    @Test
    public void testStringSchema() throws Exception {
        testSendMessages(
            10, 10, Schema.STRING,
            kv -> String.format("value-%d-%d", kv.getKey(), kv.getValue()));
    }

    private <V> void testSendMessages(int numPartitions, int numMessages,
                                      Schema<V> valueSchema,
                                      Function<KeyValue<Integer, Integer>, V> valueGenerator) throws Exception {
        provisionPartitionedTopic(PUBLIC_TENANT, numPartitions, topicName -> {
            testSendMessages(topicName, numPartitions, numMessages, valueSchema, valueGenerator);
        });
    }

    private <V> void testSendMessages(TopicName topicName, int numPartitions, int numMessages,
                                      Schema<V> valueSchema,
                                      Function<KeyValue<Integer, Integer>, V> valueGenerator) throws Exception {
        MessageRouter kafkaMessageRouter = new KafkaMessageRouter(
            HashingScheme.Murmur3_32Hash,
            0,
            true,
            1
        );
        @Cleanup
        RawKeySchemaValueProducer producer = new RawKeySchemaValueProducer(
            client, topicName.toString(), valueSchema,
            Collections.emptyMap(),
            kafkaMessageRouter
        );

        @Cleanup
        Consumer<V> valueConsumer = client.newConsumer(valueSchema)
            .topic(topicName.toString())
            .subscriptionName("value-verification")
            .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
            .subscribe();

        // send all the messages
        List<CompletableFuture<MessageId>> sendFutures = new ArrayList<>(numPartitions * numMessages);
        for (int i = 0; i < numPartitions; i++) {
            for (int j = 0; j < numMessages; j++) {
                ConsumerRecord<Object, Object> record = newKafkaRecord(
                    topicName.toString(),
                    i,
                    j,
                    valueGenerator
                );
                sendFutures.add(producer.send(record));
            }
        }
        FutureUtils.collect(sendFutures).get();

        // consume the messages
        Map<Integer, Integer> partitionIdxs = new HashMap<>();
        IntStream.range(0, numPartitions * numMessages).forEach(ignored -> {
            try {
                Message<V> message = valueConsumer.receive();
                TopicName tn = TopicName.get(message.getTopicName());
                assertEquals(topicName.toString(), tn.getPartitionedTopicName());
                int partitionIdx = tn.getPartitionIndex();
                int messageIdxInPtn = partitionIdxs.computeIfAbsent(partitionIdx, p -> -1);

                verifyPulsarMessage(
                    message,
                    partitionIdx,
                    messageIdxInPtn + 1,
                    valueGenerator
                );

                partitionIdxs.put(partitionIdx, messageIdxInPtn + 1);
            } catch (PulsarClientException e) {
                throw new RuntimeException(e);
            }
        });
    }

}
