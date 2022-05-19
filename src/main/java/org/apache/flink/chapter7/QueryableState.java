package org.apache.flink.chapter7;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.queryablestate.client.QueryableStateClient;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class QueryableState {
  public static class TrackMaximumTemperature {
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
      DataStream<Tuple2<String, Double>> tenSecsMaxTemps =
          sensorData
              // project to sensor id and temperature
              .map(reading -> Tuple2.of(reading.getId(), reading.getTemperature()))
              .returns(
                  new TupleTypeInfo<>(
                      TypeInformation.of(String.class), TypeInformation.of(Double.class)))
              // compute every 10 seconds the max temperature per sensor
              .keyBy(t -> t.f0)
              .timeWindow(Time.seconds(10))
              .max(1);

      // store latest value for each sensor in a queryable state
      tenSecsMaxTemps.keyBy(t -> t.f0).asQueryableState("maxTemperature");

      // execute application
      env.execute("Track max temperature");
    }
  }

  public static class TemperatureDashboard {
    // queryable state proxy connection information.
    // can be looked up in logs of running QueryableStateJob
    private static final String proxyHost = "127.0.0.1";
    private static final int proxyPort = 9069;
    // jobId of running QueryableStateJob.
    // can be looked up in logs of running job or the web UI
    private static final String jobId = "84072723c1b1ce132ee2dd352103ab82";

    // how many sensors to query
    private static final int numSensor = 5;
    // how often to query
    private static final int refreshInterval = 10000;

    public static void main(String[] args)
        throws IOException, ExecutionException, InterruptedException {
      // print header line of dashboard table
      String header =
          IntStream.range(0, numSensor)
              .mapToObj(i -> "sensor_" + (i + 1))
              .collect(Collectors.joining("\t| "));
      System.out.println(header);

      CompletableFuture<ValueState<Tuple2<String, Double>>>[] futures =
          new CompletableFuture[numSensor];
      Double[] results = new Double[numSensor];

      // configure client with host and port of queryable state proxy
      QueryableStateClient client = new QueryableStateClient(proxyHost, proxyPort);

      try {
        // loop forever
        while (true) {
          // send out async queries
          IntStream.range(0, numSensor)
              .forEach(i -> futures[i] = queryState("sensor_" + (i + 1), client));

          // wait for results
          for (int i = 0; i < numSensor; i++) {
            results[i] = futures[i].get().value().f1;
          }

          // print result
          String line =
              Arrays.stream(results)
                  .map(t -> String.format("%1.3f", t))
                  .collect(Collectors.joining("\t| "));
          System.out.println(line);

          // wait to send out next queries
          Thread.sleep(refreshInterval);
        }
      } finally {
        client.shutdownAndWait();
      }
    }

    private static CompletableFuture<ValueState<Tuple2<String, Double>>> queryState(
        String key, QueryableStateClient client) {
      return client.getKvState(
          JobID.fromHexString(jobId),
          "maxTemperature",
          key,
          Types.STRING,
          new ValueStateDescriptor<>(
              "", Types.TUPLE(TypeInformation.of(String.class), TypeInformation.of(Double.class))));
    }
  }
}
