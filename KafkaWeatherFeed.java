package org.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.IOUtils;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class KafkaWeatherFeed {
	static class Task extends TimerTask {
		Task() throws MalformedURLException {
			weatherURL = new URL("http://w1.weather.gov/xml/current_obs/all_xml.zip");
		}
		@Override
		public void run() {
			try {
				// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		        Properties props = new Properties();
		        props.put("metadata.broker.list", "sandbox.hortonworks.com:6667");
		        props.put("zk.connect", "localhost:2181");
//		        props.put("metadata.broker.list", "hadoop-m.c.mpcs53013-2015.internal:6667");
//		        props.put("zk.connect", "hadoop-w-1.c.mpcs53013-2015.internal:2181,hadoop-w-0.c.mpcs53013-2015.internal:2181,hadoop-m.c.mpcs53013-2015.internal:2181");
		        props.put("serializer.class", "kafka.serializer.StringEncoder");
		        props.put("request.required.acks", "1");

		        String TOPIC = "spertus-weather-events";
		        ProducerConfig config = new ProducerConfig(props);

		        Producer<String, String> producer = new Producer<String, String>(config);
				URLConnection conn = weatherURL.openConnection();
				ZipInputStream zis = new ZipInputStream(conn.getInputStream());
				ZipEntry ze = null;
				while((ze = zis.getNextEntry()) != null) {
					if(ze.getName().indexOf(".") != 4)
						continue;
					StringWriter writer = new StringWriter();
					IOUtils.copy(zis, writer);
					String weatherReport = writer.toString();					
	                KeyedMessage<String, String> data = new KeyedMessage<String, String>(TOPIC, weatherReport);
	                producer.send(data);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}	
		URL weatherURL;
	}
	public static void main(String[] args) {
		try {
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(new Task(), 0, 60*60*1000);
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
