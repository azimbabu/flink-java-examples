package org.apache.flink.chapter7;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class KeyedStateFunction {
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

    DataStream<Tuple3<String, Double, Double>> alerts =
        keyedSensorData.flatMap(new TemperatureAlertFunction(1.7));

    // print result stream to standard out
    alerts.print();

    // execute application
    env.execute("Generate Temperature Alerts");
  }

  /**
   * The function emits an alert if the temperature measurement of a sensor changed by more than a
   * configured threshold compared to the last reading.
   */
  private static class TemperatureAlertFunction
      extends RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>> {
    private final double threshold;
    private ValueState<Double> lastTempState;

    /** @param threshold The threshold to raise an alert. */
    public TemperatureAlertFunction(double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // create state descriptor
      ValueStateDescriptor<Double> lastTempDescriptor =
          new ValueStateDescriptor<>("lastTemp", Types.DOUBLE);
      // obtain the state handle
      lastTempState = getRuntimeContext().getState(lastTempDescriptor);
    }

    @Override
    public void flatMap(SensorReading reading, Collector<Tuple3<String, Double, Double>> out)
        throws Exception {
      // fetch the last temperature from state
      Double lastTemp = lastTempState.value();
      if (lastTemp == null) {
        lastTemp = 0.0;
      }

      // check if we need to emit an alert
      double tempDiff = Math.abs(reading.getTemperature() - lastTemp);
      if (tempDiff > threshold) {
        // temperature changed by more than the threshold
        out.collect(Tuple3.of(reading.getId(), reading.getTemperature(), tempDiff));
      }

      // update lastTemp state
      lastTempState.update(reading.getTemperature());
    }
  }
}
