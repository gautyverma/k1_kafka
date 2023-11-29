package com.matuga.kafka.localWSL;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCli {
  private static final Logger logger =
      LoggerFactory.getLogger(ConsumerDemoCli.class.getSimpleName());

  public static void main(String[] args) {
    logger.info("Hello World");

    String groupId = "my-java-application";
    String topic = "first_topic";

    // --- create Producer Properties ---
    Properties props = new Properties();

    // connect to LocalHost
    props.setProperty("bootstrap.servers", "127.0.0.1:9092"); // Key-value Map

    // create consumer configs
    props.setProperty("key.deserializer", StringDeserializer.class.getName());
    props.setProperty("value.deserializer", StringDeserializer.class.getName());

    props.setProperty("group.id", groupId);
    //    props.setProperty("auto.offset.reset", "none/earliest/latest");
    props.setProperty("auto.offset.reset", "earliest");

    // create a consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    // subscribe a topic
    consumer.subscribe(Arrays.asList(topic));

    // poll for data
    while (true) {
      logger.info("Polling");

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

      for (ConsumerRecord<String, String> record : records) {
        logger.info("key: " + record.key() + ", Value: " + record.value());
        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
      }
    }
  }
}
