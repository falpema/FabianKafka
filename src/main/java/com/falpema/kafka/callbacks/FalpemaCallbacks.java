package com.falpema.kafka.callbacks;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Producers con callbacks
 * @author admin
 *
 */
public class FalpemaCallbacks {
	public static final Logger log = LoggerFactory.getLogger(FalpemaCallbacks.class);

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
		props.put("acks", "all"); // todos los nodos deben dar el knowledge de recibir el mensaje
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");

		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			for (int i = 0; i < 10000; i++) {
				producer.send(new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-value"),
						new Callback() {

							// Evento que se dispara una vez entregado el mensaje
							@Override
							public void onCompletion(RecordMetadata metadata, Exception exception) {
								if (exception != null) {
									log.info("There was an error {} ", exception.getMessage());
								} else {
									log.info("Offset = {}, Partitions = {}, Topic = {}", metadata.offset(),
											metadata.partition(), metadata.topic());
								}

							}
						});

			}
			producer.flush();
		}
		log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));

	}
}
