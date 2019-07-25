package io.streamnative.connectors.kafka.pulsar;

import io.streamnative.tests.common.framework.SystemTestSuite;
import io.streamnative.tests.common.framework.SystemTestSuite.SuiteClasses;
import io.streamnative.tests.pulsar.suites.PulsarServiceTestSuite;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;

/**
 * A system test suite to test different type of pulsar producers.
 */
@RunWith(SystemTestSuite.class)
@SuiteClasses({
})
@Slf4j
public class PulsarProducerTestSuite extends PulsarServiceTestSuite {
}
