import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class HoseBirdClient {
  InputStream inputStream;
  public HoseBirdClient(){

  }

  public Client getHoseBirdClient() throws IOException {
    /**
     * Set up your blocking queues: Be sure to size these properly based on expected TPS of your
     * stream
     */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);
    /**
     * Declare the host you want to connect to, the endpoint, and authentication (basic auth or
     * oauth)
     */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<Long> followings = Lists.newArrayList(1234L, 566788L);
    List<String> terms = Lists.newArrayList("twitter", "api");
    hosebirdEndpoint.followings(followings);
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Properties secrets = getProperties();
    Authentication hosebirdAuth =
        new OAuth1(
            secrets.getProperty("twt.consumer.key"),
            secrets.getProperty("twt.consumer.secret"),
            secrets.getProperty("twt.access.token"),
            secrets.getProperty("twt.token.secret"));

    ClientBuilder builder =
        new ClientBuilder()
            .name("Hosebird-Client-01") // optional: mainly for the logs
            .hosts(hosebirdHosts)
            .authentication(hosebirdAuth)
            .endpoint(hosebirdEndpoint)
            .processor(new StringDelimitedProcessor(msgQueue))
            .eventMessageQueue(
                eventQueue); // optional: use this if you want to process client events

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  public Properties getProperties() throws IOException {
    Properties properties = new Properties();
    try {
      inputStream = getClass().getClassLoader().getResourceAsStream(Constants.PROPERTY_FILE_NAME);
      if (inputStream != null) {
        properties.load(inputStream);
      } else {
        throw new FileNotFoundException("secrets.properties does not exist");
      }
    } catch (Exception ex) {
      System.out.println(Arrays.toString(ex.getStackTrace()));
    } finally {
      inputStream.close();
    }
    return properties;
  }
}
