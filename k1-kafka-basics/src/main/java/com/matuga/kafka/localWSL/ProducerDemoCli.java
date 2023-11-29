package com.matuga.kafka.localWSL;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoCli {
  private static final Logger logger =
      LoggerFactory.getLogger(ProducerDemoCli.class.getSimpleName());

  public static void main(String[] args) {
    logger.info("Hello World");
    String topic = "first_topic";

    // --- create Producer Properties ---
    Properties props = new Properties();

    // connect to LocalHost
    props.setProperty("bootstrap.servers", "127.0.0.1:9092"); // Key-value Map

    // --- set producer properties ---
    props.setProperty("key.serializer", StringSerializer.class.getName());
    props.setProperty("value.serializer", StringSerializer.class.getName());

    // --- create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    for (int j = 0; j < 2; j++) {
      for (int i = 1; i <= 10; i++) {

        String key = "id_" + i;
        String value = "Data Input for consumer" + i;

        // --- create the Producer Record ---
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

        // --- send data ---
        producer.send(
            producerRecord,
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes everytime a record successfully sent or an exception is thrown
                if (e == null) {
                  // no exception - the record was successfully sent
                  logger.info("Key: " + key + "| Partition: " + recordMetadata.partition());

                } else {
                  logger.error("Error while producing", e);
                }
              }
            });
      }

      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    // tell the producer to send all data and block until done -- synchronous flow
    producer.flush();

    // --- flush and close the producer ---
    producer.close();
  }
}
