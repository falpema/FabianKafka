package com.falpema.kafka.multiThread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Clase para ejecutar los consumers usando hilos
 * @author admin
 *
 */
public class FalpemaMultiThreadConsumer {
	public static void main (String[] args ) {
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("group.id", "devs4j-group");
		props.setProperty("enable.auto.commit", "true");
		props.setProperty("auto.commit.interval.ms", "1000");
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		
		ExecutorService executor = Executors.newFixedThreadPool(5);
		
		for (int i =0 ; i <5 ; i++) {
			FalpemaThreadConsumer consumer = new FalpemaThreadConsumer(new KafkaConsumer<>(props));
			executor.execute(consumer);
		}
		
		while(!executor.isTerminated());
		
	}

}
