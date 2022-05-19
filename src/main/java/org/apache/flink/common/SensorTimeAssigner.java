package org.apache.flink.common;

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and emits watermarks with
 * five seconds slack.
 */
public class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<SensorReading> {
  /**
   * Configures the extractor with 5 seconds out-of-order interval.
   *
   * @param maxOutOfOrderness
   */
  public SensorTimeAssigner() {
    super(Time.seconds(5));
  }

  /**
   * Extracts timestamp from SensorReading.
   *
   * @param sensorReading sensor reading
   * @return the timestamp of the sensor reading.
   */
  @Override
  public long extractTimestamp(SensorReading sensorReading) {
    return sensorReading.getTimestamp();
  }
}
