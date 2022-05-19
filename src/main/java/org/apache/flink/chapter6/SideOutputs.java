package org.apache.flink.chapter6;

import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputs {
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
    DataStream<SensorReading> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    SingleOutputStreamOperator<SensorReading> monitoredReadings =
        readings
            // monitor stream for readings with freezing temperatures
            .process(new FreezingMonitor());

    // retrieve and print the freezing alarms
    monitoredReadings.getSideOutput(new OutputTag<String>("freezing-alarms") {}).print();

    // print the main output
    readings.print();

    env.execute();
  }

  /** Emits freezing alarms to a side output for readings with a temperature below 32F. */
  private static class FreezingMonitor extends ProcessFunction<SensorReading, SensorReading> {
    private OutputTag<String> freezingAlarmOutput;

    @Override
    public void open(Configuration parameters) throws Exception {
      freezingAlarmOutput = new OutputTag<String>("freezing-alarms") {};
    }

    @Override
    public void processElement(
        SensorReading r,
        ProcessFunction<SensorReading, SensorReading>.Context ctx,
        Collector<SensorReading> out)
        throws Exception {
      // emit freezing alarm if temperature is below 32F.
      if (r.getTemperature() < 32.0) {
        ctx.output(freezingAlarmOutput, "Freezing Alarm for" + r.getId());
      }
      // forward all readings to the regular output
      out.collect(r);
    }
  }
}
