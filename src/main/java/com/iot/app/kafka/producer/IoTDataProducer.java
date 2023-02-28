package com.iot.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.iot.app.kafka.sensor.IoTSensor;

import com.iot.app.kafka.util.IoTSensorEncoder;
import com.iot.app.kafka.util.PropertyFileReader;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static java.lang.Integer.parseInt;

@Slf4j
public class IoTDataProducer {

    private static final Properties PROPERTIES = PropertyFileReader.readPropertyFile();

    public static void main(String[] args) throws Exception {
        log.info("Property file path = " + System.getProperty("iot-kafka.properties"));

        String kafkaServer = readProperty("kafka.bootstrap-servers");
        String topic = readProperty("kafka.topic");

        log.info("Connected to Kafka cluster: " + kafkaServer + " and listening to topic: " + topic);

        // set producer properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IoTSensorEncoder.class.getName());

        // generate events
        Producer<String, IoTSensor> producer = new KafkaProducer<>(properties);
        IoTDataProducer iotProducer = new IoTDataProducer();
        iotProducer.generateIoTEvent(producer, topic);
    }

    /**
     * Method runs in while loop and generates random IoT data in JSON.
     */
    private void generateIoTEvent(Producer<String, IoTSensor> producer, String topic) throws InterruptedException {
        List<String> sensorTypes = Arrays.asList("Air humidity", "Soil humidity", "Temperature", "Light sensor", "pH soil");
        int minDelayTime = getMinDelayTime();
        int maxDelayTime = getMaxDelayTime();
        Random random = new Random();
        log.info("Sending sensor events");
        // generate event in loop

        while (true) {
            List<IoTSensor> sensorEvents = new ArrayList<>();
            for (int i = 0; i < 100; i++) { // create 100 events
                String sensorId = UUID.randomUUID().toString();
                String type = sensorTypes.get(random.nextInt(5));

                for (int j = 0; j < 5; j++) { // Add 5 events for each sensor
                    double humidity = random.nextInt(90 - 5) + 5; // random humidity between 5 and 90
                    double temperature = random.nextInt(60 - 10) + 10;// random temperature between 10 and 60

                    // The timestamp field is set during event submission to get different values across events.
                    sensorEvents.add(IoTSensor.builder()
                            .sensorId(sensorId)
                            .humidity(humidity)
                            .type(type)
                            .temperature(temperature)
                            .isOn(random.nextBoolean())
                            .build());
                }
            }
            Collections.shuffle(sensorEvents); // shuffle for random events
            for (IoTSensor event : sensorEvents) {
                event.setTimestamp(new Date());
                ProducerRecord<String, IoTSensor> data = new ProducerRecord<>(topic, event);
                producer.send(data);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime); // random delay between configured time
            }
        }
    }

    private static int getMinDelayTime() {
        return readIntProperty("sensor.min-delay");
    }

    private static int getMaxDelayTime() {
        return readIntProperty("sensor.max-delay");
    }

    private static int readIntProperty(@NonNull String property) {
        return parseInt(readProperty(property));
    }

    private static String readProperty(@NonNull String property) {
        String kafkaServer = PROPERTIES.getProperty(property);
        if (System.getProperty(property) != null) {
            kafkaServer = System.getProperty(property);
        }
        return kafkaServer;
    }
}
