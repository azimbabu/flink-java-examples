package org.apache.flink.chapter7;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class StatefulProcessFunction {
  /** main() defines and executes the DataStream program */
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000);

    // ingest sensor stream
    DataStream<SensorReading> sensorData =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(SensorReading::getId);

    SingleOutputStreamOperator<Tuple3<String, Double, Double>> alerts =
        keyedSensorData.process(new SelfCleaningTemperatureAlertFunction(1.5));

    // print result stream to standard out
    alerts.print();

    // execute application
    env.execute("Generate Temperature Alerts");
  }

  /**
   * The function emits an alert if the temperature measurement of a sensor changed by more than a
   * configured threshold compared to the last reading.
   *
   * <p>The function removes the state of a sensor if it did not receive an update within 1 hour.
   */
  private static class SelfCleaningTemperatureAlertFunction
      extends KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>> {
    private final double threshold;
    // the keyed state handle for the last temperature
    private ValueState<Double> lastTempState;
    // the keyed state handle for the last registered timer
    private ValueState<Long> lastTimerState;

    /** @param threshold The threshold to raise an alert. */
    public SelfCleaningTemperatureAlertFunction(double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // register state for last temperature
      ValueStateDescriptor<Double> lastTempDesc =
          new ValueStateDescriptor<Double>("lastTemp", Double.class);
      lastTempState = getRuntimeContext().getState(lastTempDesc);
      // register state for last timer
      ValueStateDescriptor<Long> lastTimerDesc =
          new ValueStateDescriptor<Long>("lastTimer", Long.class);
      lastTimerState = getRuntimeContext().getState(lastTimerDesc);
    }

    @Override
    public void processElement(
        SensorReading reading,
        KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>.Context ctx,
        Collector<Tuple3<String, Double, Double>> out)
        throws Exception {
      // compute timestamp of new clean up timer as record timestamp + one hour
      long newTimer = ctx.timestamp() + (3600 * 1000);
      // get timestamp of current timer
      Long currentTimer = lastTimerState.value();
      // delete previous timer and register new timer
      if (currentTimer != null) {
        ctx.timerService().deleteEventTimeTimer(currentTimer);
      }
      ctx.timerService().registerEventTimeTimer(newTimer);
      // update timer timestamp state
      lastTimerState.update(newTimer);

      // fetch the last temperature from state
      Double lastTemp = lastTempState.value();
      if (lastTemp == null) {
        lastTemp = 0.0;
      }
      // check if we need to emit an alert
      double tempDiff = Math.abs(reading.getTemperature() - lastTemp);
      if (tempDiff > threshold) {
        // temperature increased by more than the threshold
        out.collect(Tuple3.of(reading.getId(), reading.getTemperature(), tempDiff));
      }

      // update lastTemp state
      lastTempState.update(reading.getTemperature());
    }

    @Override
    public void onTimer(
        long timestamp,
        KeyedProcessFunction<String, SensorReading, Tuple3<String, Double, Double>>.OnTimerContext
            ctx,
        Collector<Tuple3<String, Double, Double>> out)
        throws Exception {
      // clear all state for the key
      lastTimerState.clear();
      lastTimerState.clear();
    }
  }
}
