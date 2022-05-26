package com.falpema.kafka.producers;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FalpemaProducerAssignSeek {

	public static final Logger log = LoggerFactory.getLogger(FalpemaProducerAssignSeek.class);

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
		props.put("acks", "all"); // todos los nodos deben dar el knowledge de recibir el mensaje
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");

		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			for (int i = 0; i < 100; i++) {
				producer.send(new ProducerRecord<String, String>("devs4j-topic", "devs4j-topic",String.valueOf(i)));

			}
			producer.flush();
		} 
		log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));

	}

}
