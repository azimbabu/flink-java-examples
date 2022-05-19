package org.apache.flink.chapter1;

import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class AverageSensorReadings {
  /**
   * main() defines and executes the DataStream program.
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //	  // create a local stream execution environment
    //	  StreamExecutionEnvironment localEnv = StreamExecutionEnvironment.createLocalEnvironment();
    //
    //	  // create a remote stream execution environment
    //	  StreamExecutionEnvironment remoteEnv = StreamExecutionEnvironment.createRemoteEnvironment(
    //	  		"host",						// hostname of JobManager
    //			  1234,						// port of JobManager process
    //			  "path/to/jarFile.jar"	// JAR file to ship to the JobManager
    //	  );

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    // ingest sensor stream
    DataStream<SensorReading> sensorData =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    DataStream<SensorReading> avgTemperature =
        sensorData
            // convert Fahrenheit to Celsius using and inlined map function
            .map(
                sensorReading ->
                    new SensorReading(
                        sensorReading.getId(),
                        sensorReading.getTimestamp(),
                        (sensorReading.getTemperature() - 32) * (5.0 / 9.0)))
            // organize stream by sensor
            .keyBy(SensorReading::getId)
            // group readings in 5 second windows
            .timeWindow(Time.seconds(5))
            // compute average temperature using a user-defined function
            .apply(new TemperatureAverager());

    // print result stream to standard out
    avgTemperature.print();

    // execute application
    env.execute("Compute average sensor temperature");
  }
}
