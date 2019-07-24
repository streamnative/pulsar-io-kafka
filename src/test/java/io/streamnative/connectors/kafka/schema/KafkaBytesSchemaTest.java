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
