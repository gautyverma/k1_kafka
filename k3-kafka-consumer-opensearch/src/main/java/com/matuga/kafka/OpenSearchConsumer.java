package com.matuga.kafka;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {
  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    String WIKIMEDIA = "wikimedia";

    // first create an OpenSearch Client
    RestHighLevelClient openSearchClient = createOpenSearchClient();

    // create our Kafka Client
    KafkaConsumer<String, String> consumer = createKafkaConsumer();

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

    // we need to create the index on openSearch if it doesn't exist already
    try (openSearchClient;
        consumer) {

      boolean indexExists =
          openSearchClient.indices().exists(new GetIndexRequest(WIKIMEDIA), RequestOptions.DEFAULT);
      if (!indexExists) {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(WIKIMEDIA);
        openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        logger.info("The Wikimedia Index has been created!");
      } else {
        logger.info("The Wikimedia Index already exits");
      }

      // subscribe the consumer
      consumer.subscribe(Collections.singleton("wikimedia.recentchange"));

      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
        int recordCount = records.count();
        logger.info("Received " + recordCount + " record(s)");

        BulkRequest bulkRequest = new BulkRequest();

        for (ConsumerRecord<String, String> record : records) {
          // send the record into openSearch

          // strategy 1 - create your own
          // define an ID using kafka Record coordinates
          //          String id = record.topic() + "_" + record.partition() + "_" + record.offset();

          try {
            // strategy 2 - extract ID from json value
            String id = extractId(record.value());

            //            IndexRequest indexRequest =
            //                new IndexRequest(WIKIMEDIA).source(record.value(), XContentType.JSON);

            // strategy 1 and startegy 2 - this implements idempotent
            IndexRequest indexRequest =
                new IndexRequest(WIKIMEDIA).source(record.value(), XContentType.JSON).id(id);

            // Commented to run bulk request
            //            IndexResponse response = openSearchClient.index(indexRequest,
            // RequestOptions.DEFAULT);
            //            log.info(response.getId());

            bulkRequest.add(indexRequest);
          } catch (Exception e) {
          }
        }

        if (bulkRequest.numberOfActions() > 0) {

          try {
            BulkResponse bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
            logger.info(
                "Inserted Bulk Response :" + bulkResponse.getItems().length + " record(s).");

            Thread.sleep(1000);

          } catch (Exception e) {
            //            e.printStackTrace();
          }
        }
        // commit offsets after the batch is consumed
        consumer.commitSync();
        logger.info("Offsets have been committed!");
      }
    } catch (WakeupException e) {
      logger.info("Consumer is starting to shutdown");
    } catch (Exception e) {
      logger.info("Unexpected expection in consumer", e);
    } finally {
      consumer.close(); // close the consumer
      openSearchClient.close();
      logger.info("The consumer is now gracefully shutdown");
    }
  }

  private static String extractId(String json) {
    // using gson
    return JsonParser.parseString(json)
        .getAsJsonObject()
        .get("meta")
        .getAsJsonObject()
        .get("id")
        .getAsString();
  }

  private static KafkaConsumer<String, String> createKafkaConsumer() {

    String groupId = "consumer-opensearch";
    String bootstrap = "127.0.0.1:9092";

    // --- create Producer Properties ---
    Properties props = new Properties();

    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap); // Key-value Map
    props.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // create a consumer
    return new KafkaConsumer<>(props);
  }

  public static RestHighLevelClient createOpenSearchClient() {

    // for local Docker
    //    String connString = "http://localhost:9200";
    String connString =
        "https://8dh3qzqqe:vy0pfrv23q@gauty-kafka-8881909683.ap-southeast-2.bonsaisearch.net:443";

    // we build a URI from the connection string
    RestHighLevelClient restHighLevelClient;
    URI connUri = URI.create(connString);
    // extract login information if it exists
    String userInfo = connUri.getUserInfo();

    if (userInfo == null) {
      // REST client without security
      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

    } else {
      // REST client with security
      String[] auth = userInfo.split(":");

      CredentialsProvider cp = new BasicCredentialsProvider();
      cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

      restHighLevelClient =
          new RestHighLevelClient(
              RestClient.builder(
                      new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                  .setHttpClientConfigCallback(
                      httpAsyncClientBuilder ->
                          httpAsyncClientBuilder
                              .setDefaultCredentialsProvider(cp)
                              .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));
    }

    return restHighLevelClient;
  }
}
