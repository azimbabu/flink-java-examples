package org.apache.flink.chapter6;

import javax.annotation.Nullable;

import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;

public class WatermarkGeneration {
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure interval of periodic watermark generation
    env.getConfig().setAutoWatermarkInterval(1000L);

    // ingest sensor stream
    DataStream<SensorReading> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource());

    DataStream<SensorReading> readingsWithPeriodicWMs =
        readings
            // assign timestamps and periodic watermarks
            .assignTimestampsAndWatermarks(new PeriodicAssigner());

    DataStream<SensorReading> readingsWithPunctuatedWMs =
        readings
            // assign timestamps and punctuated watermarks
            .assignTimestampsAndWatermarks(new PunctuatedAssigner());

    // readingsWithPeriodicWMs.print();
    readingsWithPunctuatedWMs.print();

    env.execute("Assign timestamps and generate watermarks");
  }

  /**
   * Assigns timestamps to records and provides watermarks with a 1 minute out-of-ourder bound when
   * being asked.
   */
  private static class PeriodicAssigner
      implements org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<
          SensorReading> {
    // 1 min in ms
    private final long bound = 60 * 1000;
    // the maximum observed timestamp
    private long maxTs = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
      return new Watermark(maxTs - bound);
    }

    @Override
    public long extractTimestamp(SensorReading r, long previousTS) {
      // update maximum timestamp
      maxTs = Math.max(maxTs, r.getTimestamp());
      // return record timestamp
      return r.getTimestamp();
    }
  }

  /**
   * Assigns timestamps to records and emits a watermark for each reading with sensorId ==
   * "sensor_1".
   */
  private static class PunctuatedAssigner
      implements org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<
          SensorReading> {
    // 1 min in ms
    private final long bound = 60 * 1000;

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(SensorReading r, long extractedTS) {
      if (r.getId().equals("sensor_1")) {
        // emit watermark if reading is from sensor_1
        return new Watermark(extractedTS - bound);
      } else {
        // do not emit a watermark
        return null;
      }
    }

    @Override
    public long extractTimestamp(SensorReading r, long previousTS) {
      // assign record timestamp
      return r.getTimestamp();
    }
  }
}
