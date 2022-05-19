package org.apache.flink.chapter7;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import lombok.Data;

public class BroadcastStateFunction {
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

    // define a stream of thresholds
    DataStream<ThresholdUpdate> thresholds =
        env.fromElements(
            new ThresholdUpdate("sensor_1", 5.0d),
            new ThresholdUpdate("sensor_2", 0.9d),
            new ThresholdUpdate("sensor_3", 0.5d),
            new ThresholdUpdate("sensor_1", 1.2d), // update threshold for sensor_1
            new ThresholdUpdate("sensor_3", 0.0d) // disable threshold for sensor_3
            );

    KeyedStream<SensorReading, String> keyedSensorData = sensorData.keyBy(SensorReading::getId);

    MapStateDescriptor<String, Double> broadcastStateDescriptor =
        new MapStateDescriptor<>("thresholds", Types.STRING, Types.DOUBLE);
    BroadcastStream<ThresholdUpdate> broadcastThresholds =
        thresholds.broadcast(broadcastStateDescriptor);

    // connect keyed sensor stream and broadcasted rules stream
    SingleOutputStreamOperator<Tuple3<String, Double, Double>> alerts =
        keyedSensorData
            .connect(broadcastThresholds)
            .process(new UpdatableTemperatureAlertFunction());

    // print result stream to standard out
    alerts.print();

    // execute application
    env.execute("Generate Temperature Alerts");
  }

  @Data
  private static class ThresholdUpdate {
    private final String id;
    private final double threshold;
  }

  private static class UpdatableTemperatureAlertFunction
      extends KeyedBroadcastProcessFunction<
          String, SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>> {
    // the descriptor of the broadcast state
    private MapStateDescriptor<String, Double> thresholdStateDescriptor;
    // the keyed state handle
    private ValueState<Double> lastTempState;

    @Override
    public void open(Configuration parameters) throws Exception {
      thresholdStateDescriptor =
          new MapStateDescriptor<String, Double>("thresholds", Types.STRING, Types.DOUBLE);

      // create keyed state descriptor
      ValueStateDescriptor<Double> lastTempDescriptor =
          new ValueStateDescriptor<>("lastTemp", Types.DOUBLE);
      // obtain the keyed state handle
      lastTempState = getRuntimeContext().getState(lastTempDescriptor);
    }

    @Override
    public void processElement(
        SensorReading reading,
        KeyedBroadcastProcessFunction<
                    String, SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>>
                .ReadOnlyContext
            readOnlyCtx,
        Collector<Tuple3<String, Double, Double>> out)
        throws Exception {
      // get read-only broadcast state
      ReadOnlyBroadcastState<String, Double> thresholds =
          readOnlyCtx.getBroadcastState(thresholdStateDescriptor);
      // check if we have a threshold
      if (thresholds.contains(reading.getId())) {
        // get threshold for sensor
        Double threshold = thresholds.get(reading.getId());

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
      }

      // update lastTemp state
      lastTempState.update(reading.getTemperature());
    }

    @Override
    public void processBroadcastElement(
        ThresholdUpdate update,
        KeyedBroadcastProcessFunction<
                    String, SensorReading, ThresholdUpdate, Tuple3<String, Double, Double>>
                .Context
            ctx,
        Collector<Tuple3<String, Double, Double>> out)
        throws Exception {
      // get broadcasted state handle
      BroadcastState<String, Double> thresholds = ctx.getBroadcastState(thresholdStateDescriptor);
      if (update.getThreshold() != 0.0d) {
        // configure a new threshold for the sensor
        thresholds.put(update.getId(), update.getThreshold());
      } else {
        // remove threshold for the sensor
        thresholds.remove(update.getId());
      }
    }
  }
}
