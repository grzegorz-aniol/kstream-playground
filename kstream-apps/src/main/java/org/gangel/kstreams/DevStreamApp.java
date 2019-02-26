package org.gangel.kstreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DevStreamApp {

  public static class RecordWithTimestamp {
    public String key;
    public long timestamp;
    public String value;
    public static RecordWithTimestamp builder(String key, String value) {
      RecordWithTimestamp result = new RecordWithTimestamp();
      result.key = key;
      result.value = value;
      result.timestamp = System.currentTimeMillis();
      return result;
    }
  }

  public static void main(String[] args) {
    final Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dev-stream-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, String> source = builder.stream(TopicId.PARKING_STATUS);

    final Map<String, Boolean> lastState = new HashMap<>();

    KTable<String, String> deviceStatusTable = source
        .filter((k, v) -> {
          Boolean state = "1".equals(v) ? true : false;
          Boolean prevState = lastState.get(k);
          boolean isChanged = (!state.equals(prevState));
          if (isChanged) {
            lastState.put(k, state);
          }
          return isChanged;
        })
        .groupByKey()
        .reduce((v1, v2) -> v2, Materialized.as(TopicId.PARKING_STATUS_STORE));

    deviceStatusTable.toStream().to(TopicId.PARKING_STATUS_CHANGES);

    source.groupByKey()
        .windowedBy(SessionWindows.with(Duration.ofMinutes(1)).grace(Duration.ofSeconds(30)))
        .reduce((v1, v2) -> v2)
        .toStream().to(TopicId.PARKING_STATUS_WIN1_STREAM, Produced.with(WindowedSerdes.sessionWindowedSerdeFrom(String.class), Serdes.String()));

    source.groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(30)))
        .reduce((v1, v2) -> v2)
        .toStream().to(TopicId.PARKING_STATUS_WIN2_STREAM, Produced.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), Serdes.String()));

    final KafkaStreams streams = new KafkaStreams(builder.build(), props);

    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
      @Override
      public void run() {
        System.out.println("Closing...");
        streams.close();
      }
    });

    streams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

}
