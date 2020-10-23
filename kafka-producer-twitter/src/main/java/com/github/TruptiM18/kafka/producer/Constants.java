package com.github.TruptiM18.kafka.producer;

public class Constants {
    private Constants() throws Exception {
        throw new Exception("com.github.TruptiM18.kafka.consumer.producer.Constants.class can not be initialized");
    }
    public final static String PROPERTY_FILE_NAME = "secrets.properties";
    public final static String STREAM_HOST = "https://stream.twitter.com";
    public final static String BOOTSTRAP_SERVERS="127.0.0.1:9092";
}
