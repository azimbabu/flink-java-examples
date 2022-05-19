package org.apache.flink.chapter6;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class CustomWindows {
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

    SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> countsPerThirtySecs =
        sensorData
            .keyBy(SensorReading::getId)
            // a custom window assigner for 30 second tumbling windows
            .window(new ThirtySecondsWindows())
            // a custom trigger that fires early (at most) every second
            .trigger(new OneSecondIntervalTrigger())
            // count readings per window
            .process(new CountFunction());

    countsPerThirtySecs.print();

    env.execute();
  }

  /** A custom window that groups events into 30 second tumbling windows. */
  private static class ThirtySecondsWindows extends WindowAssigner<Object, TimeWindow> {

    private static final long windowSize = 30 * 1000L;

    @Override
    public Collection<TimeWindow> assignWindows(Object r, long ts, WindowAssignerContext ctx) {
      // rounding down by 30 seconds
      long startTime = ts - (ts % windowSize);
      long endTime = startTime + windowSize;
      // emitting the corresponding time window
      return Collections.singletonList(new TimeWindow(startTime, endTime));
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
      return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
      return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
      return true;
    }
  }

  /** A trigger that fires early. The trigger fires at most every second. */
  private static class OneSecondIntervalTrigger extends Trigger<SensorReading, TimeWindow> {

    @Override
    public TriggerResult onElement(
        SensorReading r, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
      // firstSeen will be false if not set yet
      ValueState<Boolean> firstSeen =
          ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("firstSeen", Types.BOOLEAN));

      // register initial timer only for first element
      if (firstSeen.value() == null || !firstSeen.value()) {
        // compute time for next early firing by rounding watermark to second
        long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
        ctx.registerEventTimeTimer(t);
        // register timer for the window end
        ctx.registerEventTimeTimer(window.getEnd());
        firstSeen.update(true);
      }
      // Continue. Do not evaluate per element
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
        throws Exception {
      // Continue. We don't use processing time timers
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
        throws Exception {
      if (time == window.getEnd()) {
        // final evaluation and purge window state
        return TriggerResult.FIRE_AND_PURGE;
      } else {
        // register next early firing timer
        long t = ctx.getCurrentWatermark() + (1000 - (ctx.getCurrentWatermark() % 1000));
        if (t < window.getEnd()) {
          ctx.registerEventTimeTimer(t);
        }
        // fire trigger to evaluate window
        return TriggerResult.FIRE;
      }
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
      // clear trigger state
      ValueState<Boolean> firstSeen =
          ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("firstSeen", Types.BOOLEAN));
      firstSeen.clear();
    }
  }

  /**
   * A window function that counts the readings per sensor and window. The function emits the sensor
   * id, window end, time of function evaluation, and count.
   */
  private static class CountFunction
      extends ProcessWindowFunction<
          SensorReading, Tuple4<String, Long, Long, Long>, String, TimeWindow> {

    @Override
    public void process(
        String key,
        ProcessWindowFunction<SensorReading, Tuple4<String, Long, Long, Long>, String, TimeWindow>
                .Context
            ctx,
        Iterable<SensorReading> readings,
        Collector<Tuple4<String, Long, Long, Long>> out)
        throws Exception {
      // count readings
      long count = StreamSupport.stream(readings.spliterator(), false).count();
      // get current watermark
      long evalTime = ctx.currentWatermark();
      // emit result
      out.collect(Tuple4.of(key, ctx.window().getEnd(), evalTime, count));
    }
  }
}
