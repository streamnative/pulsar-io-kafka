---
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-kafka"
alias: Kafka Source Connector
---

The [Kafka](https://kafka.apache.org/) source connector pulls messages from Kafka topics and persists the messages to Pulsar topics.

![](/docs/kafka-source.png)

# How to get

This section describes how to build the Kafka source connector.

## Work with Function Worker

You can get the NAR package of the Kafka source connector from the [download page](https://github.com/streamnative/pulsar/releases/download/v2.9.2.1/pulsar-io-kafka-2.9.2.1.nar) if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

## Work with Function Mesh

You can pull the Kafka source connector Docker image from [the Docker Hub](https://hub.docker.com/r/streamnative/pulsar-io-kafka) if you use [Function Mesh](https://functionmesh.io/docs/connectors/run-connector) to run the connector.

# How to configure

Before using the Kafka source connector, you need to configure it. This table lists the properties and the descriptions.

| Name | Type| Required | Default | Description 
|------|----------|---------|-------------|-------------|
|  `bootstrapServers` |String| true | " " (empty string) | A comma-separated list of host and port pairs for establishing the initial connection to the Kafka cluster. |
| `groupId` |String| true | " " (empty string) | A unique string that identifies the group of consumer processes to which this consumer belongs. |
| `fetchMinBytes` | long|false | 1 | The minimum byte expected for each fetch response. |
| `autoCommitEnabled` | boolean |false | true | If set to true, the consumer's offset is periodically committed in the background.<br/><br/> This committed offset is used when the process fails as the position from which a new consumer begins. |
| `autoCommitIntervalMs` | long|false | 5000 | The frequency in milliseconds that the consumer offsets are auto-committed to Kafka if `autoCommitEnabled` is set to true. |
| `heartbeatIntervalMs` | long| false | 3000 | The interval between heartbeats to the consumer when using Kafka's group management facilities. <br/><br/>**Note: `heartbeatIntervalMs` must be smaller than `sessionTimeoutMs`**.|
| `sessionTimeoutMs` | long|false | 30000 | The timeout used to detect consumer failures when using Kafka's group management facility. |
| `topic` | String|true | " " (empty string)| The Kafka topic that sends messages to Pulsar. |
|  `consumerConfigProperties` | Map| false | " " (empty string) | The consumer configuration properties to be passed to consumers. <br/><br/>**Note: other properties specified in the connector configuration file take precedence over this configuration**. |
| `keyDeserializationClass` | String|false | org.apache.kafka.common.serialization.StringDeserializer | The deserializer class for Kafka consumers to deserialize keys.<br/> The deserializer is set by a specific implementation of [`KafkaAbstractSource`](https://github.com/apache/pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaAbstractSource.java).
| `valueDeserializationClass` | String|false | org.apache.kafka.common.serialization.ByteArrayDeserializer | The deserializer class for Kafka consumers to deserialize values.
| `autoOffsetReset` | String | false | earliest | The default offset reset policy. |
## Work with Function Worker

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

**Example**

- JSON 

   ```json
  {
    "bootstrapServers": "pulsar-kafka:9092",
    "groupId": "test-pulsar-io",
    "topic": "my-topic",
    "sessionTimeoutMs": "10000",
    "autoCommitEnabled": false
  }
   ```

- YAML

   ```yaml
    configs:
      bootstrapServers: "pulsar-kafka:9092"
      groupId: "test-pulsar-io"
      topic: "my-topic"
      sessionTimeoutMs: "10000"
      autoCommitEnabled: false
    ```

## Work with Function Mesh

You can create a [CustomResourceDefinitions (CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to create a Kafka source connector. Using CRD makes Function Mesh naturally integrate with the Kubernetes ecosystem. For more information about Pulsar source CRD configurations, see [source CRD configurations](https://functionmesh.io/docs/connectors/io-crd-config/source-crd-config).

You can define a CRD file (YAML) to set the properties as below.

```yaml
apiVersion: compute.functionmesh.io/v1alpha1
kind: Sink
metadata:
  name: kafka-source-sample
spec:
  image: streamnative/pulsar-io-kafka:2.9.2.9
  className: org.apache.pulsar.io.kafka.KafkaBytesSource
  replicas: 1
  maxReplicas: 1
  forwardSourceMessageProperty: true
  output:
    producerConf:
      maxPendingMessages: 1000
      maxPendingMessagesAcrossPartitions: 50000
      useThreadLocalProducers: true
    topic: persistent://public/default/destination
    typeClassName: java.nio.ByteBuffer
  sourceConfig:
    bootstrapServers: "pulsar-kafka:9092"
    groupId: "test-pulsar-io"
    topic: "my-topic"
    sessionTimeoutMs: "10000"
    autoCommitEnabled: false
  pulsar:
    pulsarConfig: "test-pulsar-sink-config"
  resources:
    limits:
    cpu: "0.2"
    memory: 1.1G
    requests:
    cpu: "0.1"
    memory: 1G
  java:
    jar: connectors/pulsar-io-kafka-2.9.2.9.nar
  clusterName: test-pulsar
```

# How to use

You can use the Kafka source connector with Function Worker or Function Mesh.

## Work with Function Worker

You can make the Kafka source connector as a Pulsar built-in connector and use it on a standalone cluster or an on-premises cluster.

### Standalone cluster

This example describes how to use the Kafka source connector to feed data from Kafka and write data to Pulsar topics in the standalone mode.

#### Prerequisites

- Install [Docker](https://docs.docker.com/get-docker/) (Community Edition).

#### Steps

1. Download and start the Confluent Platform.

    For details, see the [documentation](https://docs.confluent.io/platform/current/quickstart/ce-docker-quickstart.html#step-1-download-and-start-cp) to install the Kafka service locally.

2. Pull a Pulsar image and start Pulsar in standalone mode.
   
    ```bash
    docker pull apachepulsar/pulsar:latest
      
    docker run -d -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data --name pulsar-kafka-standalone apachepulsar/pulsar:latest bin/pulsar standalone
    ```

3. Create a producer file _kafka-producer.py_.
   
   ```python
   from kafka import KafkaProducer
   producer = KafkaProducer(bootstrap_servers='localhost:9092')
   future = producer.send('my-topic', b'hello world')
   future.get()
   ```

4. Create a consumer file _pulsar-client.py_.

    ```python
    import pulsar

    client = pulsar.Client('pulsar://localhost:6650')
    consumer = client.subscribe('my-topic', subscription_name='my-aa')

    while True:
        msg = consumer.receive()
        print msg
        print dir(msg)
        print("Received message: '%s'" % msg.data())
        consumer.acknowledge(msg)

    client.close()
    ```

5. Copy the following files to Pulsar.
   
    ```bash
    docker cp pulsar-io-kafka.nar pulsar-kafka-standalone:/pulsar
    docker cp kafkaSourceConfig.yaml pulsar-kafka-standalone:/pulsar/conf
    ```

6. Open a new terminal window and start the Kafka source connector in local run mode.

    ```bash
    docker exec -it pulsar-kafka-standalone /bin/bash

    ./bin/pulsar-admin source localrun \
    --archive ./pulsar-io-kafka.nar \
    --tenant public \
    --namespace default \
    --name kafka \
    --destination-topic-name my-topic \
    --source-config-file ./conf/kafkaSourceConfig.yaml \
    --parallelism 1
    ```

7. Open a new terminal window and run the Kafka producer locally.

    ```bash
    python3 kafka-producer.py
    ```

8. Open a new terminal window and run the Pulsar consumer locally.

    ```bash
    python3 pulsar-client.py
    ```

    The following information appears on the consumer terminal window.

    ```bash
    Received message: 'hello world'
    ```

### On-premises cluster

This example explains how to create a Kafka source connector in an on-premises cluster.

1. Copy the NAR package of the Kafka connector to the Pulsar connectors directory.

    ```
    cp pulsar-io-kafka-{{connector:version}}.nar $PULSAR_HOME/connectors/pulsar-io-kafka-{{connector:version}}.nar
    ```

2. Reload all [built-in connectors](https://pulsar.apache.org/docs/en/next/io-connectors/).

    ```
    PULSAR_HOME/bin/pulsar-admin sources reload
    ```

3. Check whether the Kafka source connector is available on the list or not.

    ```
    PULSAR_HOME/bin/pulsar-admin sources available-sources
    ```

4. Create a Kafka source connector on a Pulsar cluster using the [`pulsar-admin sources create`](http://pulsar.apache.org/tools/pulsar-admin/2.8.0-SNAPSHOT/#-em-create-em--14) command.

    ```
    PULSAR_HOME/bin/pulsar-admin sources create \
    --source-config-file <kafka-source-config.yaml>
    ```

## Work with Function Mesh

This example describes how to create a Kafka source connector for a Kuberbetes cluster using Function Mesh.

### Prerequisites

- Create and connect to a [Kubernetes cluster](https://kubernetes.io/).
- Create a [Pulsar cluster](https://pulsar.apache.org/docs/en/kubernetes-helm/) in the Kubernetes cluster.
- [Install the Function Mesh Operator and CRD](https://functionmesh.io/docs/install-function-mesh/) into the Kubernetes cluster.

### Steps

1. Define the Kafka source connector with a YAML file and save it as `source-sample.yaml`.

    This example shows how to publish the Kafka source connector to Function Mesh with a Docker image.

    ```yaml
    apiVersion: compute.functionmesh.io/v1alpha1
    kind: Sink
    metadata:
      name: kafka-source-sample
    spec:
      image: streamnative/pulsar-io-kafka:{{connector:version}}
      className: org.apache.pulsar.io.kafka.KafkaBytesSource
      replicas: 1
      maxReplicas: 1
      forwardSourceMessageProperty: true
      output:
        producerConf:
          maxPendingMessages: 1000
          maxPendingMessagesAcrossPartitions: 50000
          useThreadLocalProducers: true
        topic: persistent://public/default/destination
        typeClassName: java.nio.ByteBuffer
      sourceConfig:
        bootstrapServers: "pulsar-kafka:9092"
        groupId: "test-pulsar-io"
        topic: "my-topic"
        sessionTimeoutMs: "10000"
        autoCommitEnabled: false
      pulsar:
        pulsarConfig: "test-pulsar-sink-config"
      resources:
        limits:
        cpu: "0.2"
        memory: 1.1G
        requests:
        cpu: "0.1"
        memory: 1G
      java:
        jar: connectors/pulsar-io-kafka-{{connector:version}}.nar
      clusterName: test-pulsar
    ```

2. Apply the YAML file to create the Kafka source connector.

    **Input**

    ```
    kubectl apply -f <path-to-source-sample.yaml>
    ```

    **Output**

    ```
    source.compute.functionmesh.io/kafka-source-sample created
    ```

3. Check whether the Kafka source connector is created successfully.

    **Input**

    ```
    kubectl get all
    ```

    **Output**

    ```
    NAME                                         READY   STATUS      RESTARTS   AGE
    pod/kafka-source-sample-0               1/1    Running     0          77s
    ```

    After confirming that it was created successfully, you can produce and consume messages using the Kafka source connector between Pulsar and Kafka.
