## Pulsar IO Kafka Connector

pulsar-io-kafka is a [Pulsar IO Connector](http://pulsar.apache.org/docs/en/io-overview/) for copying data between
Kafka and Pulsar.

> NOTE: This connector is an enhanced version of `pulsar-io-kafka` connector to support schema.

### Get started

This provides a step-by-step example how to use this Kafka source connector to copy *json* data from a Kafka topic
to a Pulsar topic and save the data in *AVRO* format.

#### Build pulsar-io-kafka connector

1. Git clone `pulsar-io-kafka`. Assume *PULSAR_IO_KAFKA_HOME* is the home directory for your
   cloned `pulsar-io-kafka` repo for the remaining steps.
   ```
   $ git clone https://github.com/streamnative/pulsar-io-kafka
   ```

2. Build the connector in `${PULSAR_IO_KAFKA_HOME}` directory.
   ```
   mvn clean install -DskipTests
   ```
   After successfully built the connector, a *NAR* package is generated under *target* directory. The *NAR* package
   is the one you used for 
   ```
   $ ls target/pulsar-io-kafka-2.5.0-SNAPSHOT.nar
   target/pulsar-io-kafka-2.5.0-SNAPSHOT.nar
   ```

#### Prepare a config for running pulsar-io-kafka connector

An example yaml config is available [here](https://github.com/streamnative/pulsar-io-kafka/blob/master/conf/pulsar-io-kafka.yaml)

This example yaml config is used for copying json data from Kafka topic *input-topic* to Pulsar topic *output-topic* and
save the messages in AVRO format.

#### Run pulsar-io-kafka connector

1. Download Pulsar 2.4.0 release from [Pulsar website](http://pulsar.apache.org/en/download/) and follow
   the [instructions](http://pulsar.apache.org/docs/en/standalone/) to start a standalone Pulsar.
   Assume *PULSAR_HOME* is the home directory for your Pulsar installation for the remaining steps.

   Example command to start a standalone Pulsar.
   ```
   cd ${PULSAR_HOME}
   bin/pulsar standalone
   ```

2. Download Kafka release from [Kafka website](http://kafka.apache.org/downloads) and follow
   the [instructions](http://kafka.apache.org/quickstart) to start a Kafka server.
   Assume *KAFKA_HOME* is the home directory for your Kafka installation for the remaining steps.

   Example commands to start a Kafka server.
   ```
   cd ${KAFKA_HOME}
   bin/zookeeper-server-start.sh config/zookeeper.properties
   bin/kafka-server-start.sh config/server.properties
   ```

3. Create a Kafka topic.
   
   > Make sure the Kafka topic name is the one you configured in `pulsar-io-kafka.yaml`.

   ```
   bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --create --topic input-topic --replication-factor 1 --partitions 4
   ```

4. Copy the pulsar-io-kafka conenctor to `${PULSAR_HOME}/connectors` directory.

   ```
   cd ${PULSAR_HOME}
   mkdir -p connectors
   cp ${PULSAR_IO_KAFKA_HOME}/target/pulsar-io-kafka-2.5.0-SNAPSHOT.nar connectors/
   ```

5. Localrun the pulsar-io-kafka connector.

   > NOTE: `--destination-topic-name` is used by pulsar io runtime but not by this `pulsar-io-kafka` connector. We can not omit this 
   >       setting at this momement. So you can given any *unused* topic name for now.

   ```
   cd ${PULSAR_HOME}
   bin/pulsar-admin sources localrun -a connectors/pulsar-io-kafka-2.5.0-SNAPSHOT.nar --tenant public --namespace default --name test-kafka-source --source-config-file ${PULSAR_IO_KAFKA_HOME}/conf/pulsar-io-kafka.yaml --destination-topic-name test-kafka-source-topic
   ```
   Once the connector is running, keep this terminal open during the remaining steps.


6. Now we can use a *json* kafka producer and an *avro* pulsar consumer to verify if the connector is working as expected.

   Start a *json* Kafka producer to produce 100 messages.
   ```
   cd ${PULSAR_IO_KAFKA_HOME}
   bin/kafka-json-producer.sh localhost:9092 input-topic 100
   ```

   Start an *avro* Pulsar consumer to consume the 100 messages (in avro format).
   ```
   cd ${PULSAR_IO_KAFKA_HOME}
   bin/pulsar-avro-consumer.sh pulsar://localhost:6650 output-topic sub
   ```

   You will see similar output in the terminal you run `pulsar-avro-consumer.sh`:
   ```
   Receive message : key = user-99, value = User(name=user-99, age=990, address=address-99)
   ```
