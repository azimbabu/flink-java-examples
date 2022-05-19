package org.apache.flink.chapter6;

import java.util.Random;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class LateDataHandling {
  private static OutputTag<SensorReading> lateReadingsOutput = new OutputTag<>("late-readings") {};

  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000L);

    // ingest sensor stream and shuffle timestamps to produce out-of-order records
    DataStream<SensorReading> outOfOrderReadings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // shuffle timestamps by max 7 seconds to generate late data
            .map(new TimestampShuffler(7 * 1000))
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    // Different strategies to handle late records.
    // Select and uncomment on of the lines below to demonstrate a strategy.

    // 1. Filter out late readings (to a side output) using a ProcessFunction
    // filterLateReadings(outOfOrderReadings);

    // 2. Redirect late readings to a side output in a window operator
    // sideOutputLateEventsWindow(outOfOrderReadings);

    // 3. Update results when late readings are received in a window operator
    updateForLateEventsWindow(outOfOrderReadings);

    env.execute();
  }

  /**
   * Count reading per tumbling window and update results if late readings are received. Print
   * results.
   */
  private static void updateForLateEventsWindow(DataStream<SensorReading> readings) {
    SingleOutputStreamOperator<Tuple4<String, Long, Long, String>> countPer10Secs =
        readings
            .keyBy(SensorReading::getId)
            .timeWindow(Time.seconds(10))
            .allowedLateness(Time.seconds(5))
            .process(new UpdatingWindowCountFunction());

    // print results
    countPer10Secs.print();
  }

  private static void sideOutputLateEventsWindow(DataStream<SensorReading> readings) {
    SingleOutputStreamOperator<Tuple3<String, Long, Long>> countPer10Secs =
        readings
            .keyBy(SensorReading::getId)
            .timeWindow(Time.seconds(10))
            // emit late readings to a side output
            .sideOutputLateData(lateReadingsOutput)
            // count readings per window
            .process(new CountFunction());

    // retrieve the late events from the side output as a stream
    DataStream<SensorReading> lateStream = countPer10Secs.getSideOutput(lateReadingsOutput);
    // print messages for late readings
    lateStream.map(r -> "*** late reading ***" + r.getId()).print();

    // print results
    countPer10Secs.print();
  }

  /** Filter late readings to a side output and print the on-time and late streams. */
  private static void filterLateReadings(DataStream<SensorReading> readings) {
    // re-direct late readings to the side output
    SingleOutputStreamOperator<SensorReading> filteredReadings =
        readings.process(new LateReadingsFilter());

    // retrieve late readings
    DataStream<SensorReading> lateReadings = filteredReadings.getSideOutput(lateReadingsOutput);

    // print the filtered stream
    filteredReadings.print();

    // print messages for late readings
    lateReadings.map(r -> "*** late reading ***" + r.getId()).print();
  }

  /**
   * A MapFunction to shuffle (up to a max offset) the timestamps of SensorReadings to produce
   * out-of-order events.
   */
  private static class TimestampShuffler implements MapFunction<SensorReading, SensorReading> {

    private final Random random;
    private final int maxRandomOffset;

    public TimestampShuffler(int maxRandomOffset) {
      this.maxRandomOffset = maxRandomOffset;
      this.random = new Random();
    }

    @Override
    public SensorReading map(SensorReading r) throws Exception {
      long shuffleTs = r.getTimestamp() + random.nextInt(maxRandomOffset);
      return new SensorReading(r.getId(), shuffleTs, r.getTemperature());
    }
  }

  private static class CountFunction
      extends ProcessWindowFunction<SensorReading, Tuple3<String, Long, Long>, String, TimeWindow> {
    @Override
    public void process(
        String id,
        ProcessWindowFunction<SensorReading, Tuple3<String, Long, Long>, String, TimeWindow>.Context
            ctx,
        Iterable<SensorReading> elements,
        Collector<Tuple3<String, Long, Long>> out)
        throws Exception {
      long count = StreamSupport.stream(elements.spliterator(), false).count();
      out.collect(Tuple3.of(id, ctx.window().getEnd(), count));
    }
  }

  /**
   * A ProcessFunction that filters out late sensor readings and re-directs them to a side output
   */
  private static class LateReadingsFilter extends ProcessFunction<SensorReading, SensorReading> {
    @Override
    public void processElement(
        SensorReading r,
        ProcessFunction<SensorReading, SensorReading>.Context ctx,
        Collector<SensorReading> out)
        throws Exception {
      // compare record timestamp with current watermark
      if (r.getTimestamp() < ctx.timerService().currentWatermark()) {
        // this is a late reading => redirect it to the side output
        ctx.output(lateReadingsOutput, r);
      } else {
        out.collect(r);
      }
    }
  }

  /** A counting WindowProcessFunction that distinguishes between first results and updates. */
  private static class UpdatingWindowCountFunction
      extends ProcessWindowFunction<
          SensorReading, Tuple4<String, Long, Long, String>, String, TimeWindow> {
    @Override
    public void process(
        String id,
        ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, String>, String, TimeWindow>
                .Context
            ctx,
        Iterable<SensorReading> elements,
        Collector<Tuple4<String, Long, Long, String>> out)
        throws Exception {
      // count the number of readings
      long count = StreamSupport.stream(elements.spliterator(), false).count();

      // state to check if this is the first evaluation of the window or not
      ValueState<Boolean> isUpdate =
          ctx.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdate", Types.BOOLEAN));
      if (isUpdate.value() != null && !isUpdate.value()) {
        // first evaluation, emit first result
        out.collect(Tuple4.of(id, ctx.window().getEnd(), count, "first"));
        isUpdate.update(true);
      } else {
        // not the first evaluation, emit an update
        out.collect(Tuple4.of(id, ctx.window().getEnd(), count, "update"));
      }
    }
  }
}
