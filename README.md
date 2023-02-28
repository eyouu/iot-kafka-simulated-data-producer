# IoT Kafka Producer
IoT Kafka Producer is a Maven application for generating IoT Data events using Apache Kafka. This project requires following tools and technologies.

- JDK - 1.8
- Maven - 3.3.9
- ZooKeeper - 3.5.9
- Kafka - 3.3.0

## Default properties
```properties
# Kafka properties
kafka.bootstrap-servers=localhost:9092
kafka.topic=iot-data-event

# Delay of received events in milliseconds
sensor.min-delay=1000
sensor.max-delay=2000
```

You can build and run this application using below commands. Please check resources/iot-kafka.properties for configuration details.

```sh
mvn package
mvn exec:java -Dexec.mainClass="com.iot.app.kafka.producer.IoTDataProducer"
```

Alternate way to run this application is using the “iot-kafka-simulated-data-producer-1.0.0.jar” file

```sh
java -jar iot-kafka-simulated-data-producer-1.0.0.jar
```

Example of usage with overriding kafka server

```sh
java -Dkafka.bootstrap-servers=ec2-3-71-4-166.eu-central-1.compute.amazonaws.com:9092 -jar iot-kafka-simulated-data-producer-1.0.0.jar
```