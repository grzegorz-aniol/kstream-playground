package org.gangel.kstreams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SimpleProducerApp {

  public static final String PARKING_STATUS = "parking_status";

  public static void main(String[] args) {

    // Create the Properties class to instantiate the Consumer with the desired settings:
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    props.put("buffer.memory", 33554432);
    props.put("compression.type", "none");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("client.id", "");
    props.put("linger.ms", 0);
    props.put("max.block.ms", 1000);
    props.put("max.request.size", 1048576);
    props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
    props.put("request.timeout.ms", 3000);
    props.put("timeout.ms", 200);
    props.put("max.in.flight.requests.per.connection", 5);
    props.put("retry.backoff.ms", 5);

    KafkaProducer<String, String> myProducer = new KafkaProducer<String, String>(props);
    DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
    final var rnd = new Random();
    Supplier<Long> nextTsFun = () -> System.currentTimeMillis() + rnd.nextLong() % 15000;

    final int NUM_OF_DEV = 10;
    final double SIGNALS_PER_SECOND = 1;
    final long TOTAL_OPS = (long) (NUM_OF_DEV * SIGNALS_PER_SECOND);
    final long sleepTimer = 1000 / TOTAL_OPS; // how long you want to wait before the next record to be sent

    final ArrayList<Boolean> devStatus = IntStream.rangeClosed(1, NUM_OF_DEV)
        .boxed()
        .map(it -> false)
        .collect(Collectors.toCollection(ArrayList<Boolean>::new));
    final ArrayList<Long> nextChange = devStatus.stream()
        .map(it -> nextTsFun.get())
        .collect(Collectors.toCollection(ArrayList<Long>::new));

    long cnt = 0;
    long lastUpdate = System.currentTimeMillis();
    try {
      while (true) {
        var device = (int) (cnt % NUM_OF_DEV);
        var key = Long.toString(device);
        var value = devStatus.get(device) ? "1" : "0";
        myProducer.send(new ProducerRecord<String, String>(TopicId.PARKING_STATUS, key, value));
        ++cnt;

        for(int i=0; i < NUM_OF_DEV; ++i) {
          long ts = System.currentTimeMillis();
          if (nextChange.get(i) < ts) {
            devStatus.set(i, !devStatus.get(i));
            nextChange.set(i, nextTsFun.get());
          }
        }

        Thread.sleep(sleepTimer);
        if (System.currentTimeMillis() - lastUpdate > Duration.ofSeconds(10).toMillis()) {
          lastUpdate = System.currentTimeMillis();
          System.out.printf("Generated messages: %d\n", cnt);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      myProducer.close();
    }

  }
}
