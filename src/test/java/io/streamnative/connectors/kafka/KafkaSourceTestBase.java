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
package io.streamnative.connectors.kafka;

import static io.streamnative.tests.pulsar.service.ExternalServices.KAFKA;
import static io.streamnative.tests.pulsar.service.ExternalServices.KAFKA_SCHEMA_REGISTRY;

import io.streamnative.tests.common.framework.Service;
import io.streamnative.tests.pulsar.service.PulsarService;
import io.streamnative.tests.pulsar.suites.PulsarServiceSystemTestCase;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.net.ServiceURI;

/**
 * A test base for running kafka source tests.
 */
public abstract class KafkaSourceTestBase extends PulsarServiceSystemTestCase {

    protected Service kafkaService;
    protected Service schemaRegistryService;
    protected ServiceURI kafkaServiceUri;
    protected ServiceURI schemaRegistryServiceUri;

    @Override
    protected Consumer<byte[]> createConsumer(String topic, String subscription) throws PulsarClientException {
        return super.createConsumer(topic, subscription);
    }

    public KafkaSourceTestBase(PulsarService service) {
        super(service);
    }

    @Override
    protected void doSetup() throws Exception {
        super.doSetup();

        this.kafkaService = pulsarService().getExternalServiceOperator()
            .getService(KAFKA);
        this.schemaRegistryService = pulsarService().getExternalServiceOperator()
            .getService(KAFKA_SCHEMA_REGISTRY);

        this.kafkaServiceUri = ServiceURI.create(kafkaService.getServiceUris().get(0));
        this.schemaRegistryServiceUri = ServiceURI.create(schemaRegistryService.getServiceUris().get(0));
    }

}
