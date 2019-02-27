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

/**
 * Produce endless stream of records that simulate IoT sensor values.
 * Each sensor may generate olny one of two values: 0 or 1
 */
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

    final int numOfSensors = 10;
    final double sensorSignalsPerSec = 1;
    final long totalOPS = (long) (numOfSensors * sensorSignalsPerSec);
    final long sleepTimer = 1000 / totalOPS; // how long you want to wait before the next record to be sent

    final ArrayList<Boolean> devStatus = IntStream.rangeClosed(1, numOfSensors)
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
        // pick up next sensor and sends its current indication
        var device = (int) (cnt % numOfSensors);
        var key = Long.toString(device);
        var value = devStatus.get(device) ? "1" : "0";
        myProducer.send(new ProducerRecord<String, String>(TopicId.IOT_STATUS, key, value));
        ++cnt;

        // check if any sensor need to change its value in this iteration
        for(int i=0; i < numOfSensors; ++i) {
          long ts = System.currentTimeMillis();
          if (nextChange.get(i) < ts) {
            devStatus.set(i, !devStatus.get(i));
            nextChange.set(i, nextTsFun.get()); // calculate when the value is going to change next time
          }
        }

        // control throughput
        Thread.sleep(sleepTimer);

        // log every 10 sec number of generates messages
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
