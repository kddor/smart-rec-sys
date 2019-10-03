package com.data.niu.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class Property {

	private final static String CONF_NAME = "config.properties";

	private static Properties contextProperties;

	static {
		InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
		contextProperties = new Properties();
		try {
			InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
			contextProperties.load(inputStreamReader);
		} catch (IOException e) {
			System.err.println(">>>nearline-layer-rec<<<资源文件加载失败!");
			e.printStackTrace();
		}
		System.out.println(">>>nearline-layer-rec<<<资源文件加载成功");
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public static String getStrValue(String key) {
		return contextProperties.getProperty(key);
	}

	/**
	 *
	 * @param key
	 * @return
	 */
	public static int getIntValue(String key) {
		String strValue = getStrValue(key);
		return Integer.parseInt(strValue);
	}

	/**
	 * 获取kafka配置文件
	 * @param groupId
	 * @return
	 */
	public static Properties getKafkaProperties(String groupId) {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"));
		properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"));
		properties.setProperty("group.id", groupId);
		return properties;
	}

	public static void main(String args[]){
		System.out.println(Property.getStrValue("hbase.rootdir"));
		System.out.println(Property.getStrValue("kafka.zookeeper.connect"));
	}

}