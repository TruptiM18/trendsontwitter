public final class Constants {
  private Constants() throws Exception {
    throw new Exception("Can not instantiate Constants.class");
  }

  public static final String STREAM_HOST =
      "https://stream.twitter.com/1.1/statuses/filter.json?track=twitter";
  public static final String PROPERTY_FILE_NAME="secrets.properties";
}
