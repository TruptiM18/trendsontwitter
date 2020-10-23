package com.github.TruptiM18.kafka.consumer;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ElasticSearchConsumer {
  public static RestHighLevelClient createClient() throws IOException {
    Properties credentialProperties = getBonsaiCredentialsProperties();

    String hostname = credentialProperties.getProperty("bonsai.host");
    String username = credentialProperties.getProperty("bonsai.username");
    String password = credentialProperties.getProperty("bonsai.password");
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(
        AuthScope.ANY, new UsernamePasswordCredentials(username, password));
    RestClientBuilder restClientBuilder =
        RestClient.builder(new HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback(
                httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
    RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
    return client;
  }

  public static KafkaConsumer<String, String> createConsumer(String topic) {
    String bootstrapServer = "localhost:9092";
    String groupId = "kafka-demo-elasticsearch";
    Properties consumerProperties = getConsumerProperties(bootstrapServer, groupId);
    KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
    kafkaConsumer.subscribe(Collections.singleton(topic));
    return kafkaConsumer;
  }

  public static Properties getBonsaiCredentialsProperties() throws IOException {
    InputStream inputStream;
    Properties properties = new Properties();
    inputStream =
        ElasticSearchConsumer.class
            .getClassLoader()
            .getResourceAsStream("bonsai.credentials.properties");
    if (inputStream != null) {
      properties.load(inputStream);
    } else {
      throw new FileNotFoundException("bonsai.credentials.properties does not exist");
    }
    inputStream.close();
    return properties;
  }

  public static Properties getConsumerProperties(String bootstrapServer, String groupId) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"50");
    return properties;
  }

  public static String getTwitterIdFromRecord(String recordString) {
    return JsonParser.parseString(recordString).getAsJsonObject().get("id_str").getAsString();
  }

  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    RestHighLevelClient client = createClient();
    KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
    // poll for new data
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      logger.info("Received " + records.count() + " records");
      BulkRequest bulkRequest=new BulkRequest();
      for (ConsumerRecord<String, String> record : records) {
        IndexRequest request = new IndexRequest("twitter2");
        // to make consumer idempotent
        /* There are two strategies
        1. Kafka Generic ID
        String id=record.topic()+"_"+record.partition()+"_"+record.offset();
        2. Use unique ID of the record you are saving in that topic.say tweet_id
        */
        logger.info(getTwitterIdFromRecord(record.value()));
        //use following API to search the inserted record
        ///GET - twitter2/_doc/<twitterId>
        request.id(getTwitterIdFromRecord(record.value()));
        request.source(record.value(), XContentType.JSON);
        bulkRequest.add(request);
      }
      if (!records.isEmpty()) {
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        logger.info("Committing offsets... ");
        consumer.commitSync();
        logger.info("Commit was successful");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {

        }
      }

    }
    // client.close();
  }
}
