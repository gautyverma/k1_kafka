package com.matuga.kafka.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {
  private static final Logger logger = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

  public static void main(String[] args) {
    logger.info("Hello World");

    // --- create Producer Properties ---
    Properties props = new Properties();

    // connect to LocalHost
    //     props.setProperty("bootstrap.servers", "127.0.0.1:9092"); // Key-value Map

    // connected to remote server - console.upstash.com
    props.setProperty("bootstrap.servers", "suited-chipmunk-8384-eu2-kafka.upstash.io:9092");
    props.setProperty("sasl.mechanism", "SCRAM-SHA-256");
    props.setProperty("security.protocol", "SASL_SSL");
    props.setProperty(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"c3VpdGVkLWNoaXBtdW5rLTgzODQkOpf6LylBbwXU1SzWSJ-P-glFTbitXXz8mEg\" password=\"ZmQ3MjgwN2ItOGY2NC00NmI1LWE1ZDYtOTlkMjU4MWVhZDky\";");

    // --- set producer properties ---
    props.setProperty("key.serializer", StringSerializer.class.getName());
    props.setProperty("value.serializer", StringSerializer.class.getName());

    // --- create the Producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    // --- create the Producer Record ---
    // topic - "topic-gauty-v1" that you have created on remote location
    ProducerRecord<String, String> producerRecord =
        new ProducerRecord<>("topic-gauty-v1", "hello_gautam_1");

    // --- send data ---
    producer.send(producerRecord);

    // tell the producer to send all data and block until done -- synchronous flow
    producer.flush();

    // --- flush and close the producer ---
    producer.close();
  }
}
