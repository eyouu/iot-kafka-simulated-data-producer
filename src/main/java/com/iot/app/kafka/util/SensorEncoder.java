package com.iot.app.kafka.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@Slf4j
public class SensorEncoder<T> implements Serializer<T> {

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T sensor) {
        try {
            String msg = objectMapper.writeValueAsString(sensor);
            log.info(msg);
            return msg.getBytes();
        } catch (JsonProcessingException e) {
            log.error("Error in Serialization", e);
        }
        return null;
    }
}
