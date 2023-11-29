package com.matuga.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithShutdown {
  private static final Logger logger =
      LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

  public static void main(String[] args) {
    logger.info("Hello World");

    String groupId = "my-java-application";
    String topic = "topic-gauty-v1";

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

    // create consumer configs
    props.setProperty("key.deserializer", StringDeserializer.class.getName());
    props.setProperty("value.deserializer", StringDeserializer.class.getName());

    props.setProperty("group.id", groupId);
    //    props.setProperty("auto.offset.reset", "none/earliest/latest");
    props.setProperty("auto.offset.reset", "earliest");

    // create a consumer
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    // Shutdown hook
    // get a reference to the main thread
    final Thread mainThread = Thread.currentThread();

    // adding the shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread() {
              public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                  mainThread.join();
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
            });

    try {
      // subscribe a topic
      consumer.subscribe(Arrays.asList(topic));

      // poll for data
      while (true) {

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

        for (ConsumerRecord<String, String> record : records) {
          logger.info("key: " + record.key() + ", Value: " + record.value());
          logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
        }
      }
    } catch (WakeupException e) {
      logger.info("Consumer is starting to shutdown");
    } catch (Exception e) {
      logger.info("Unexpected expection in consumer", e);
    } finally {
      consumer.close();
      logger.info("The consumer is now gracefully shutdown");
    }
  }
}
