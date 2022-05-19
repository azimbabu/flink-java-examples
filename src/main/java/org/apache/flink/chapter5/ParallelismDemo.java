package org.apache.flink.chapter5;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ParallelismDemo {
  public static void main(String[] args) {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    System.out.println("Default Parallelism: " + env.getParallelism());

    env.setParallelism(32);

    System.out.println("Default Parallelism: " + env.getParallelism());
  }
}
