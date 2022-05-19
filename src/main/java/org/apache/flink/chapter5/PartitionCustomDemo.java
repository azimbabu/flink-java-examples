package org.apache.flink.chapter5;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PartitionCustomDemo {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    DataStream<Tuple1<Integer>> numbers =
        env.fromElements(
            Tuple1.of(-1), Tuple1.of(2), Tuple1.of(3), Tuple1.of(-4), Tuple1.of(-6), Tuple1.of(5));

    DataStream<Tuple1<Integer>> partitioned =
        numbers.partitionCustom(
            (Partitioner<Integer>)
                (key, numPartitions) -> {
                  if (key < 0) {
                    return 0;
                  } else {
                    return ThreadLocalRandom.current().nextInt(numPartitions);
                  }
                },
            0);

    partitioned.print();

    // execute the application
    env.execute("PartitionCustomDemo Example");
  }
}
