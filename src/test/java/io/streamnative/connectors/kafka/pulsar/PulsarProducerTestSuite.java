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
    KeyValueSchemaProducerTest.class,
    MultiVersionKeyValueSchemaProducerTest.class,
    RawKeySchemaValueProducerTest.class
})
@Slf4j
public class PulsarProducerTestSuite extends PulsarServiceTestSuite {
}
