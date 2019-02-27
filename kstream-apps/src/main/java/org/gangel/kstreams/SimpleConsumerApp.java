package org.gangel.kstreams;

import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * Simple consumer of Kafka topic.
 * The topic need to have key and value serialized as String
 */
public class SimpleConsumerApp {

  public static void main(String[] args) throws ParseException {

    Options options = new Options();
    options.addOption("t", "topic", true, "Topic name");
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse( options, args);

    String topicName = Optional.ofNullable(cmd.getOptionValue("t")).orElse(TopicId.PARKING_STATUS);

    // Create the Properties class to instantiate the Consumer with the desired settings:
    Properties props = new Properties();
    props.put("group.id", SimpleConsumerApp.class.getName() + "_" + topicName);
    props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    props.put("fetch.min.bytes", 1);
//    props.put("group.id", "");
    props.put("max.block.ms", 1000);
    props.put("max.request.size", 1048576);
    props.put("fetch.max.wait.ms", 500);
    props.put("heartbeat.interval.ms", 3000);
    props.put("max.partition.fetch.bytes", 1048576);
    props.put("max.poll.records", 100);
    props.put("max.poll.interval.ms", 2000);
    props.put("session.timeout.ms", 30000);
    props.put("auto.offset.reset", "latest");
    props.put("connections.max.idle.ms", 540000);
    props.put("enable.auto.commit", true);
    props.put("exclude.internal.topics", true);
    props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
    props.put("request.timeout.ms", 40000);
    props.put("auto.commit.interval.ms", 5000);
    props.put("metadata.max.age.ms", 300000);
    props.put("reconnect.backoff.ms", 50);
    props.put("retry.backoff.ms", 100);
    props.put("client.id", "");

    // Create a KafkaConsumer instance and configure it with properties.
    KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

    myConsumer.subscribe(List.of(topicName));

    long cnt = 0;
    while (true){
      var rec = myConsumer.poll(Duration.ofMillis(200));
      if (rec != null && rec.count() > 0) {
        cnt += rec.count();
        var currentTime = System.currentTimeMillis();
        rec.forEach(cr -> {
          System.out.println(String.format("Topic: %s, time: %d, message time: %d, key: %s, value: %s", topicName, currentTime, cr.timestamp(), cr.key(), cr.value()));
        });
      }
    }

  }

}
