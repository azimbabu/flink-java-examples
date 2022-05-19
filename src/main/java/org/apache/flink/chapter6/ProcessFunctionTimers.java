package org.apache.flink.chapter6;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class ProcessFunctionTimers {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    // ingest sensor stream
    DataStream<SensorReading> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource());

    DataStream<String> warnings =
        readings
            // key by sensor id
            .keyBy(SensorReading::getId)
            // apply ProcessFunction to monitor temperatures
            .process(new TempIncreaseAlertFunction());

    warnings.print();

    env.execute("Monitor sensor temperatures.");
  }

  /**
   * Emits a warning if the temperature of a sensor monotonically increases for 1 second (in
   * processing time).
   */
  private static class TempIncreaseAlertFunction
      extends KeyedProcessFunction<String, SensorReading, String> {
    // hold temperature of last sensor reading
    private ValueState<Double> lastTemp;

    // hold timestamp of currently active timer
    private ValueState<Long> currentTimer;

    @Override
    public void open(Configuration parameters) throws Exception {
      lastTemp = getRuntimeContext().getState(new ValueStateDescriptor<>("lastTemp", Types.DOUBLE));
      currentTimer =
          getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Types.LONG));
    }

    @Override
    public void processElement(
        SensorReading r,
        KeyedProcessFunction<String, SensorReading, String>.Context ctx,
        Collector<String> out)
        throws Exception {

      // get previous temperature
      Double prevTemp = lastTemp.value();
      if (prevTemp == null) {
        prevTemp = 0.0;
      }
      // update last temperature
      lastTemp.update(r.getTemperature());

      Long currentTimerTimestamp = currentTimer.value();
      if (currentTimerTimestamp == null) {
        currentTimerTimestamp = 0L;
      }

      if (prevTemp == 0.0) {
        // first sensor reading for this key.
        // we cannot compare it with a previous value.
      } else if (r.getTemperature() < prevTemp) {
        // temperature decreased. Delete current timer.
        ctx.timerService().deleteProcessingTimeTimer(currentTimerTimestamp);
        currentTimer.clear();
      } else if (r.getTemperature() > prevTemp && currentTimerTimestamp == 0) {
        // temperature increased and we have not set a timer yet.
        // set timer for now + 1 second
        long timerTs = ctx.timerService().currentProcessingTime() + 1000;
        ctx.timerService().registerProcessingTimeTimer(timerTs);
        // remember current timer
        currentTimer.update(timerTs);
      }
    }

    @Override
    public void onTimer(
        long ts,
        KeyedProcessFunction<String, SensorReading, String>.OnTimerContext ctx,
        Collector<String> out)
        throws Exception {
      out.collect(
          "Temperature of sensor '"
              + ctx.getCurrentKey()
              + "' monotonically increased for 1 second.");
      // reset current timer
      currentTimer.clear();
    }
  }
}
