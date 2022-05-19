package org.apache.flink.chapter5;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Example program to demonstrate keyed transformation functions: keyBy, reduce. */
public class KeyedTransformations {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    rollingAggregateDemo(env);

    rollingReduceDemo(env);

    // ingest sensor stream
    DataStream<SensorReading> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    // group sensor readings by sensor id
    KeyedStream<SensorReading, String> keyed = readings.keyBy(r -> r.getId());

    // a rolling reduce that computes the highest temperature of each sensor and
    // the corresponding timestamp
    DataStream<SensorReading> maxTempPerSensor =
        keyed.reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r1 : r2);

    maxTempPerSensor.print();

    // execute application
    env.execute("Keyed Transformations Example");
  }

  private static void rollingReduceDemo(StreamExecutionEnvironment env) {
    DataStream<Tuple2<String, List<String>>> inputStream =
        env.fromElements(
            Tuple2.of("en", Arrays.asList("tea")),
            Tuple2.of("fr", Arrays.asList("vin")),
            Tuple2.of("en", Arrays.asList("cake")));

    DataStream<Tuple2<String, List<String>>> resultStream =
        inputStream
            .keyBy(0)
            .reduce(
                (x, y) ->
                    Tuple2.of(
                        x.f0,
                        Stream.concat(x.f1.stream(), y.f1.stream()).collect(Collectors.toList())));
    resultStream.print();
  }

  private static void rollingAggregateDemo(StreamExecutionEnvironment env) {
    DataStream<Tuple3> inputStream =
        env.fromElements(
            Tuple3.of(1, 2, 2), Tuple3.of(2, 3, 1), Tuple3.of(2, 2, 4), Tuple3.of(1, 5, 3));

    DataStream<Tuple3> resultStream = inputStream.keyBy(0).sum(1);

    resultStream.print();
  }
}
