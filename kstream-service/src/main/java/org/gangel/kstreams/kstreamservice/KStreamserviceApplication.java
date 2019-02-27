package org.gangel.kstreams.kstreamservice;

import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableBinding(KafkaStreamsProcessor.class)
public class KStreamserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KStreamserviceApplication.class, args);
	}

  @StreamListener("input")
  @SendTo("output")
  public KStream<String, String> processIotSignals(KStream<String, String> source) {
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
        .reduce((v1, v2) -> v2, Materialized.as(TopicId.IOT_STAT_TABLE));

    return deviceStatusTable.toStream();
  }

}
