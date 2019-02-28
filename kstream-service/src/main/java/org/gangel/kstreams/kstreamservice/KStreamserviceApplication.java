package org.gangel.kstreams.kstreamservice;

import com.hazelcast.cache.HazelcastCacheManager;
import com.hazelcast.core.HazelcastInstance;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.binder.kafka.streams.annotations.KafkaStreamsProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.handler.annotation.SendTo;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableBinding(KafkaStreamsProcessor.class)
public class KStreamserviceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KStreamserviceApplication.class, args);
	}

	private Logger log = LoggerFactory.getLogger(KStreamserviceApplication.class);

	@Autowired
  private StateService stateService;

  @StreamListener("input")
  @SendTo("output")
  public KStream<String, String> processIotSignals(KStream<String, String> source) {
//    final Map<String, Boolean> lastState = new HashMap<>();

    KTable<String, String> deviceStatusTable = source
        .filter((k, v) -> {
          Boolean state = "1".equals(v) ? true : false;
          //Boolean prevState = lastState.get(k);
          Boolean prevState = stateService.getState(k);
          boolean isChanged = (!state.equals(prevState));
          if (isChanged) {
            // lastState.put(k, state);
            log.info("State is changed. Key={}, prev value={}, new value={}", k, prevState, state);
            stateService.updateState(k, state);
          }
          return isChanged;
        })
        .groupByKey()
        .reduce((v1, v2) -> v2, Materialized.as(TopicId.IOT_STAT_TABLE));

    return deviceStatusTable.toStream();
  }


}
