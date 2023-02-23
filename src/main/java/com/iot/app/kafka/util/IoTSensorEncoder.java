package com.iot.app.kafka.util;

import com.iot.app.kafka.vo.IoTSensor;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class IoTSensorEncoder implements Encoder<IoTSensor> {
	
	private static final Logger logger = Logger.getLogger(IoTSensorEncoder.class);
	private static ObjectMapper objectMapper = new ObjectMapper();

	public IoTSensorEncoder(VerifiableProperties verifiableProperties) {
    }

	public byte[] toBytes(IoTSensor iotEvent) {
		try {
			String msg = objectMapper.writeValueAsString(iotEvent);
			logger.info(msg);
			return msg.getBytes();
		} catch (JsonProcessingException e) {
			logger.error("Error in Serialization", e);
		}
		return null;
	}
}
