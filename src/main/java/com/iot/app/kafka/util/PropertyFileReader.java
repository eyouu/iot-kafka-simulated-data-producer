package com.iot.app.kafka.util;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class PropertyFileReader {

	private static Properties prop = new Properties();

	public static Properties readPropertyFile() {
		try {
			if (prop.isEmpty()) {
				InputStream input = PropertyFileReader.class.getClassLoader().getResourceAsStream("iot-kafka.properties");
				try {
					prop.load(input);
				} catch (IOException ex) {
					log.error(ex.getMessage());
					throw ex;
				} finally {
					if (input != null) {
						input.close();
					}
				}
			}
			return prop;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}
}