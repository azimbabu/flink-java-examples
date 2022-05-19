package org.apache.flink.common;

import java.util.Calendar;
import java.util.Random;
import java.util.stream.IntStream;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * Flink SourceFunction to generate SensonReadings with random temperature values.
 *
 * <p>Each parallel instance of the source simulates 10 sensors which emit one sensor reading every
 * 100 ms.
 *
 * <p>Note : This is a simple data-generating source function that does not checkpoint its state. In
 * case of a failure, the source does not replay any data.
 */
public class SensorSource extends RichParallelSourceFunction<SensorReading> {
  // flag indicating whether source is still running
  private boolean running = true;

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. * */
  @Override
  public void run(SourceContext<SensorReading> sourceContext) throws Exception {
    // initialize random number generator
    Random random = new Random();
    // look up index of this parallel task
    long taskIndex = getRuntimeContext().getIndexOfThisSubtask();

    // initialize sensor ids and temperatures
    String[] sensorIds = new String[10];
    double[] temperaturesInF = new double[10];
    IntStream.range(0, 10)
        .forEach(
            i -> {
              sensorIds[i] = "sensor_" + (taskIndex * 10 + i);
              temperaturesInF[i] = 65 + (random.nextGaussian() * 20);
            });

    while (running) {
      // get current time
      long currentTime = Calendar.getInstance().getTimeInMillis();

      // emit SensorReadings
      IntStream.range(0, 10)
          .forEach(
              i -> {
                // update current temperature
                temperaturesInF[i] += random.nextGaussian() * 0.5;
                // emit reading
                sourceContext.collect(
                    new SensorReading(sensorIds[i], currentTime, temperaturesInF[i]));
              });

      // wait for 100 ms
      Thread.sleep(100);
    }
  }

  /** Cancels this SourceFunction. * */
  @Override
  public void cancel() {
    running = false;
  }
}
