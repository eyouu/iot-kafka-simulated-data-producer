package com.iot.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.iot.app.kafka.sensor.*;

import com.iot.app.kafka.util.SensorEncoder;
import com.iot.app.kafka.util.PropertyFileReader;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import static com.iot.app.kafka.util.IdGenerator.*;
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
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SensorEncoder.class.getName());

        IoTDataProducer ioTDataProducer = new IoTDataProducer();
        ioTDataProducer.generateEvents(properties, topic);
    }

    /**
     * Method runs in while loop and generates random IoT data in JSON.
     */
    private void generateEvents(Properties properties, String topic) throws InterruptedException {
        Producer<String, AirHumiditySensor> airHumidityProducer = new KafkaProducer<>(properties);
        Producer<String, LightSensor> lightSensorProducer = new KafkaProducer<>(properties);
        Producer<String, PhSoilSensor> phSoilProducer = new KafkaProducer<>(properties);
        Producer<String, SoilHumiditySensor> soilHumidityProducer = new KafkaProducer<>(properties);
        Producer<String, TemperatureSensor> temperatureSensorProducer = new KafkaProducer<>(properties);

        int minDelayTime = getMinDelayTime();
        int maxDelayTime = getMaxDelayTime();
        Random random = new Random();
        log.info("Sending sensor events");
        while (true) {
            List<AirHumiditySensor> airHumiditySensors = generateAirHumiditySensor(random);
            List<LightSensor> lightSensors = generateLightSensor(random);
            List<PhSoilSensor> phSoilSensors = generatePhSoilSensor(random);
            List<SoilHumiditySensor> soilHumiditySensors = generateSoilHumiditySensor(random);
            List<TemperatureSensor> temperatureSensors = generateTemperatureSensor(random);

            for (int i = 0; i < 20; i++) {
                AirHumiditySensor airHumidityEvent = airHumiditySensors.get(i);
                airHumidityEvent.setTimestamp(new Date());
                ProducerRecord<String, AirHumiditySensor> airHumidityRecord = new ProducerRecord<>(topic, airHumidityEvent);
                airHumidityProducer.send(airHumidityRecord);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime);

                LightSensor lightSensorEvent = lightSensors.get(i);
                lightSensorEvent.setTimestamp(new Date());
                ProducerRecord<String, LightSensor> lightSensorRecord = new ProducerRecord<>(topic, lightSensorEvent);
                lightSensorProducer.send(lightSensorRecord);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime);

                PhSoilSensor phSoilSensorEvent = phSoilSensors.get(i);
                phSoilSensorEvent.setTimestamp(new Date());
                ProducerRecord<String, PhSoilSensor> phSoilRecord = new ProducerRecord<>(topic, phSoilSensorEvent);
                phSoilProducer.send(phSoilRecord);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime);

                SoilHumiditySensor soilHumiditySensorEvent = soilHumiditySensors.get(0);
                soilHumiditySensorEvent.setTimestamp(new Date());
                ProducerRecord<String, SoilHumiditySensor> soilHumidityRecord = new ProducerRecord<>(topic, soilHumiditySensorEvent);
                soilHumidityProducer.send(soilHumidityRecord);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime);

                TemperatureSensor temperatureSensorEvent = temperatureSensors.get(0);
                temperatureSensorEvent.setTimestamp(new Date());
                ProducerRecord<String, TemperatureSensor> temperatureSensorRecord = new ProducerRecord<>(topic, temperatureSensorEvent);
                temperatureSensorProducer.send(temperatureSensorRecord);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime);
            }
        }
    }

    private static List<AirHumiditySensor> generateAirHumiditySensor(Random random) {
        List<AirHumiditySensor> events = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            AirHumiditySensor event = AirHumiditySensor.builder()
                    .id(AIR_HUMIDITY_ID_LIST.get(random.nextInt(20)))
                    .value(String.valueOf(random.nextInt(90 - 5) + 5)) // from 5 to 90
                    .unit("%")
                    .build();
            events.add(event);
        }
        return events;
    }

    private static List<LightSensor> generateLightSensor(Random random) {
        List<LightSensor> events = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            LightSensor event = LightSensor.builder()
                    .id(LIGHT_SENSOR_ID_LIST.get(random.nextInt(20)))
                    .lightIntensity(random.nextInt(900 - 50) + 50)
                    .batteryLevel(random.nextInt(100 - 1) + 1)
                    .signalStrength(random.nextInt(100 - 30) + 30)
                    .build();
            events.add(event);
        }
        return events;
    }

    private static List<PhSoilSensor> generatePhSoilSensor(Random random) {
        List<PhSoilSensor> events = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            PhSoilSensor event = PhSoilSensor.builder()
                    .id(PH_SOIL_ID_LIST.get(random.nextInt(20)))
                    .phLevel(random.nextInt(14))
                    .isCalibrated(random.nextBoolean())
                    .batteryLevel(random.nextInt(100 - 1) + 1)
                    .signalStrength(random.nextInt(100 - 30) + 30)
                    .build();
            events.add(event);
        }
        return events;
    }

    private static List<SoilHumiditySensor> generateSoilHumiditySensor(Random random) {
        List<String> firmwareVersions = Arrays.asList("version: 1", "version: 2", "version: 3");
        List<SoilHumiditySensor> events = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            SoilHumiditySensor event = SoilHumiditySensor.builder()
                    .id(SOIL_HUMIDITY_ID_LIST.get(random.nextInt(20)))
                    .humidity(random.nextInt(90 - 5) + 5)
                    .signalStrength(random.nextInt(100 - 30) + 30)
                    .firmwareVersion(firmwareVersions.get(random.nextInt(3)))
                    .build();
            events.add(event);
        }
        return events;
    }

    private static List<TemperatureSensor> generateTemperatureSensor(Random random) {
        List<TemperatureSensor> events = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            TemperatureSensor event = TemperatureSensor.builder()
                    .id(TEMPERATURE_SENSOR_ID_LIST.get(random.nextInt(20)))
                    .temperature(random.nextInt(60 - 10) + 10)
                    .batteryLevel(random.nextInt(100 - 1) + 1)
                    .build();
            events.add(event);
        }
        return events;
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
