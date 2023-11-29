package com.matuga.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangesProducer {
  private static final Logger logger = LoggerFactory.getLogger(WikimediaChangesProducer.class);

  public static void main(String[] args) throws InterruptedException {

    String bootstrapServers = "127.0.0.1:9092";

    // create Producer properties
    Properties props = new Properties();

    // connect to LocalHost
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // Key-value Map

    // --- set producer properties ---
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // set high throughput producer configs
    props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    /*
        // set safe producer configs (kafka <= 2.8)

        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        props.setProperty(ProducerConfig.ACKS_CONFIG,"all"); // same as settings -1
        props.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
    */

    // create the Producer properties
    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    String topic = "wikimedia.recentchange";

    EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
    ;
    String url = "https://stream.wikimedia.org/v2/stream/recentchange";
    EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
    EventSource eventSource = builder.build();

    // start the producer in another thread
    eventSource.start();

    // we produce for 5 minutes and block the program until then
    TimeUnit.MINUTES.sleep(5);
  }
}
