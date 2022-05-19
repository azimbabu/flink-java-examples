package org.apache.flink.chapter1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import lombok.Data;

public class WordCountDemo {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000);

    DataStreamSource<String> lines =
        env.fromElements(
            "Apache Flink is a community-driven open source framework for distributed big data analytics,",
            "like Hadoop and Spark. The core of Apache Flink is a distributed streaming dataflow engine written");

    lines
        .flatMap(
            (FlatMapFunction<String, Tuple2<String, Integer>>)
                (line, out) -> {
                  String[] words = line.split("\\W+");
                  for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                  }
                })
        .returns(
            new TupleTypeInfo<>(
                TypeInformation.of(String.class), TypeInformation.of(Integer.class)))
        .keyBy(0)
        .sum(1)
        .print();

    lines
        .flatMap(
            (FlatMapFunction<String, String>)
                (line, out) -> {
                  String[] words = line.split("\\W+");
                  for (String word : words) {
                    out.collect(word);
                  }
                })
        .returns(Types.STRING)
        .map(word -> new WordCount(word, 1))
        .keyBy(wordCount -> wordCount.getWord())
        .reduce(
            (wordCount1, wordCount2) ->
                new WordCount(wordCount1.getWord(), wordCount1.getCount() + wordCount2.getCount()))
        .print();
    env.execute();
  }

  @Data
  private static class WordCount {
    private final String word;
    private final int count;
  }
}
