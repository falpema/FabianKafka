package com.falpema.kafka.transaccional;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.falpema.kafka.producers.FalpemaProducer;

public class TransactionalProducer {
	public static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092"); // Broker de kafka al que nos vamos a conectar
		props.put("acks", "all"); // todos los nodos deben dar el knowledge de recibir el mensaje
		props.put("transactional.id", "devs4j-producer-id");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("linger.ms", "10");

		try (Producer<String, String> producer = new KafkaProducer<>(props);) {
			try {
				producer.initTransactions();
				producer.beginTransaction();
				for (int i = 0; i < 100000; i++) {
					producer.send(
							new ProducerRecord<String, String>("devs4j-topic", String.valueOf(i), "devs4j-value"));
					
					if (i == 50000) {  // simular error
						throw new Exception("unexpected Exception");
					}
				}

				producer.commitTransaction();
				producer.flush();
			} catch (Exception e) { // si hay algun problema se aborta la transaccion
				log.error("Error ", e);
				producer.abortTransaction();
			}
		}
		log.info("Processing time = {} ms ", (System.currentTimeMillis() - startTime));

		/**
		 * PARAMETROS IMPORTANTES BATCH-SIZE -> Define tamaño de grupos de mensajes para
		 * kafka BUFFER.MEMORY -> Tamaño maximo del buffer en el que se pueden colocar
		 * los batches LINGER.MS -> Define cada cuanto ms se hace el envio de estos
		 * mensajes
		 */
	}
}
