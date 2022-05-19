package org.apache.flink.chapter1;

import org.apache.flink.common.SensorReading;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TemperatureAverager
    implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {

  /**
   * apply() is invoked once for each window.
   *
   * @param sensorId the key (sensorId) of the window
   * @param timeWindow meta data for the window
   * @param input an iterable over the collected sensor readings that were assigned to the window
   * @param output a collector to emit results from the function
   * @throws Exception
   */
  @Override
  public void apply(
      String sensorId,
      TimeWindow timeWindow,
      Iterable<SensorReading> input,
      Collector<SensorReading> output)
      throws Exception {
    // compute the average temperature
    int count = 0;
    double sum = 0.0;
    for (SensorReading sensorReading : input) {
      count++;
      sum += sensorReading.getTemperature();
    }
    double avgTemp = sum / count;

    // emit a SensorReading with the average temperature
    output.collect(new SensorReading(sensorId, timeWindow.getEnd(), avgTemp));
  }
}
