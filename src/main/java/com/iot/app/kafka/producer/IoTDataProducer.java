package com.iot.app.kafka.producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import com.iot.app.kafka.vo.IoTSensor;
import org.apache.log4j.Logger;

import com.iot.app.kafka.util.PropertyFileReader;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class IoTDataProducer {

    private static final Logger logger = Logger.getLogger(IoTDataProducer.class);
    private static Properties properties = PropertyFileReader.readPropertyFile();

    public static void main(String[] args) throws Exception {
        // read config file
        String zookeeper = properties.getProperty("com.iot.app.kafka.zookeeper");
        if (System.getProperty("com.iot.app.kafka.zookeeper") != null) {
            zookeeper = System.getProperty("com.iot.app.kafka.zookeeper");
        }
        String brokerList = properties.getProperty("com.iot.app.kafka.brokerlist");
        if (System.getProperty("com.iot.app.kafka.brokerlist") != null) {
            brokerList = System.getProperty("com.iot.app.kafka.brokerlist");
        }
        String topic = properties.getProperty("com.iot.app.kafka.topic");
        if (System.getProperty("com.iot.app.kafka.topic") != null) {
            topic = System.getProperty("com.iot.app.kafka.topic");
        }
        logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

        // set producer properties
        Properties properties = new Properties();
        properties.put("zookeeper.connect", zookeeper);
        properties.put("metadata.broker.list", brokerList);
        properties.put("request.required.acks", "1");
        properties.put("serializer.class", "com.iot.app.kafka.util.IoTSensorEncoder");
        //generate event
        Producer<String, IoTSensor> producer = new Producer<>(new ProducerConfig(properties));
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
        logger.info("Sending sensor events");
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
                KeyedMessage<String, IoTSensor> data = new KeyedMessage<>(topic, event);
                producer.send(data);
                Thread.sleep(random.nextInt(maxDelayTime - minDelayTime) + maxDelayTime); // random delay between configured time
            }
        }
    }

    private static int getMinDelayTime() {
        int minDelay = Integer.parseInt(properties.getProperty("iot.sensor.minDelay"));
        if (System.getProperty("iot.sensor.minDelay") != null) {
            minDelay = Integer.parseInt(System.getProperty("iot.sensor.minDelay"));
        }
        return minDelay;
    }

    private static int getMaxDelayTime() {
        int maxDelay = Integer.parseInt(properties.getProperty("iot.sensor.maxDelay"));
        if (System.getProperty("iot.sensor.maxDelay") != null) {
            maxDelay = Integer.parseInt(System.getProperty("iot.sensor.maxDelay"));
        }
        return maxDelay;
    }
}
