package com.github.TruptiM18.kafka.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Producer {
  private Logger logger;
  private InputStream inputStream;

  public Producer() {
    logger = LoggerFactory.getLogger(Producer.class.getName());
  }

  public static void main(String[] args) throws IOException {
    Producer producer = new Producer();
    producer.read();
  }

  public void read() throws IOException {
    logger.info("setup");
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // optional: set up some followings and track terms
    hosebirdEndpoint.trackTerms(Lists.newArrayList("bitcoin", "usa", "work from home"));

    // these secrets should be read from a config file
    Properties secrets = getSecretProperties();
    Authentication hosebirdAuth =
        new OAuth1(
            secrets.getProperty("twt.consumer.key"),
            secrets.getProperty("twt.consumer.secret"),
            secrets.getProperty("twt.access.token"),
            secrets.getProperty("twt.token.secret"));

    // build client
    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01") // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue));
    final Client client = builder.build();

    // connect the client
    client.connect();

    // get kafka producer
    final KafkaProducer<String, String> producer = getKafkaProducer();

    // add shutdown hook
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                new Runnable() {
                  public void run() {
                    logger.info("shutting down application");
                    logger.info("closing connection");
                    client.stop();
                    logger.info("closing producer");
                    producer.close();
                    logger.info("done!");
                  }
                }));

    // do whatever needs to be done with messages
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException exception) {
        exception.printStackTrace();
        client.stop();
      }
      if (msg != null) {
        logger.info(msg);
        producer.send(
            new ProducerRecord<String, String>("twitter_tweets", null, msg),
            new Callback() {
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                  // record was sent successfully
                  logger.info(
                      "New metadata received.\n"
                          + "Topic: "
                          + recordMetadata.topic()
                          + "\nPartition: "
                          + recordMetadata.partition()
                          + "\nOffset: "
                          + recordMetadata.offset()
                          + "\nTimestamp: "
                          + recordMetadata.timestamp());
                } else {
                  // exception occurred
                  logger.error("Error while producing: " + e);
                }
              }
            });
      }
    }
    logger.info("application closed");
  }

  public KafkaProducer getKafkaProducer() {
    Properties kafkaProducer = getKafkaProperties();
    // create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kafkaProducer);
    return producer;
  }

  public Properties getKafkaProperties() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVERS);
    properties.setProperty(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    // high throughput producer (at the expense of a bit of latency and CPU usage
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
    return properties;
  }

  public Properties getSecretProperties() throws IOException {
    Properties properties = new Properties();
    inputStream = Producer.class.getClassLoader().getResourceAsStream(Constants.PROPERTY_FILE_NAME);
    if (inputStream != null) {
      properties.load(inputStream);
    } else {
      throw new FileNotFoundException("secrets.properties does not exist");
    }
    return properties;
  }
}
