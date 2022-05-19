package org.apache.flink.chapter6;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import lombok.Data;

public class WindowFunctions {

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    // ingest sensor stream
    DataStream<SensorReading> sensorData =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    DataStream<Tuple2<String, Double>> minTempPerWindow =
        sensorData
            .map(r -> Tuple2.of(r.getId(), r.getTemperature()))
            .returns(
                new TupleTypeInfo<>(
                    TypeInformation.of(String.class), TypeInformation.of(Double.class)))
            .keyBy(t -> t.f0)
            .timeWindow(Time.seconds(15))
            .reduce((r1, r2) -> Tuple2.of(r1.f0, Double.min(r1.f1, r2.f1)));

    DataStream<Tuple2<String, Double>> avgTempPerWindow =
        sensorData
            .map(r -> Tuple2.of(r.getId(), r.getTemperature()))
            .returns(
                new TupleTypeInfo<>(
                    TypeInformation.of(String.class), TypeInformation.of(Double.class)))
            .keyBy(r -> r.f0)
            .timeWindow(Time.seconds(15))
            .aggregate(new AvgTempFunction());

    // output the lowest and highest temperature reading every 5 seconds
    DataStream<MinMaxTemp> minMaxTempPerWindow =
        sensorData
            .keyBy(SensorReading::getId)
            .timeWindow(Time.seconds(5))
            .process(new HighAndLowTempProcessFunction());

    DataStream<MinMaxTemp> minMaxTempPerWindow2 =
        sensorData
            .map(r -> Tuple3.of(r.getId(), r.getTemperature(), r.getTemperature()))
            .returns(
                new TupleTypeInfo<>(
                    TypeInformation.of(String.class),
                    TypeInformation.of(Double.class),
                    TypeInformation.of(Double.class)))
            .keyBy(t -> t.f0)
            .timeWindow(Time.seconds(5))
            .reduce(
                (r1, r2) ->
                    // incrementally compute min and max temperature
                    Tuple3.of(r1.f0, Double.min(r1.f1, r2.f1), Double.max(r1.f2, r2.f2)),
                new AssignWindowEndProcessFunction());

    // print result stream
    minMaxTempPerWindow2.print();

    env.execute();
  }

  // An AggregateFunction to compute the average tempeature per sensor.
  // The accumulator holds the sum of temperatures and an event count.
  private static class AvgTempFunction
      implements AggregateFunction<
          Tuple2<String, Double>, Tuple3<String, Double, Integer>, Tuple2<String, Double>> {
    @Override
    public Tuple3<String, Double, Integer> createAccumulator() {
      return Tuple3.of("", 0.0, 0);
    }

    @Override
    public Tuple3<String, Double, Integer> add(
        Tuple2<String, Double> in, Tuple3<String, Double, Integer> acc) {
      return Tuple3.of(in.f0, in.f1 + acc.f1, 1 + acc.f2);
    }

    @Override
    public Tuple2<String, Double> getResult(Tuple3<String, Double, Integer> acc) {
      return Tuple2.of(acc.f0, acc.f1 / acc.f2);
    }

    @Override
    public Tuple3<String, Double, Integer> merge(
        Tuple3<String, Double, Integer> acc1, Tuple3<String, Double, Integer> acc2) {
      return Tuple3.of(acc1.f0, acc1.f1 + acc2.f1, acc1.f2 + acc2.f2);
    }
  }

  @Data
  private static class MinMaxTemp {
    private final String id;
    private final Double min;
    private final Double max;
    private final Long endTs;
  }

  /**
   * A ProcessWindowFunction that computes the lowest and highest temperature reading per window and
   * emits them together with the end timestamp of the window.
   */
  private static class HighAndLowTempProcessFunction
      extends ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow> {
    @Override
    public void process(
        String key,
        ProcessWindowFunction<SensorReading, MinMaxTemp, String, TimeWindow>.Context ctx,
        Iterable<SensorReading> vals,
        Collector<MinMaxTemp> out)
        throws Exception {
      Stream<Double> temps =
          StreamSupport.stream(vals.spliterator(), false).map(SensorReading::getTemperature);
      Double min = temps.min(Double::compareTo).get();
      Double max = temps.max(Double::compareTo).get();
      long windowEnd = ctx.window().getEnd();
      out.collect(new MinMaxTemp(key, min, max, windowEnd));
    }
  }

  private static class AssignWindowEndProcessFunction
      extends ProcessWindowFunction<
          Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow> {
    @Override
    public void process(
        String key,
        ProcessWindowFunction<Tuple3<String, Double, Double>, MinMaxTemp, String, TimeWindow>
                .Context
            ctx,
        Iterable<Tuple3<String, Double, Double>> minMaxIt,
        Collector<MinMaxTemp> out)
        throws Exception {
      Tuple3<String, Double, Double> minMax = minMaxIt.iterator().next();
      long windowEnd = ctx.window().getEnd();
      out.collect(new MinMaxTemp(key, minMax.f1, minMax.f2, windowEnd));
    }
  }
}
