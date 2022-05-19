package org.apache.flink.chapter8;

import java.io.ObjectInputStream;
import java.io.Serializable;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.chapter8.util.DerbySetup;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;

import scala.concurrent.ExecutionContext;
import scala.concurrent.ExecutionContextExecutor;

/**
 * Example program that demonstrates the use of an AsyncFunction to enrich records with data that is
 * stored in an external database. The AsyncFunction queries the database via its JDBC interface.
 * For this demo, the database is an embedded, in-memory Derby database.
 *
 * <p>The AsyncFunction sends queries and handles their results asynchronously in separate threads
 * for improved latency and throughput.
 *
 * <p>The program includes a MapFunction that performs the same logic as the AsyncFunction in a
 * synchronous fashion. You can compare the behavior of the synchronous MapFunction and the
 * AsyncFunction by commenting out parts of the code.
 */
public class AsyncFunctionExample {
  public static void main(String[] args) throws Exception {
    DerbySetup.setupDerby(
        "CREATE TABLE SensorLocations (" + "sensor VARCHAR(16) PRIMARY KEY, room VARCHAR(16))");

    // insert some initial data
    DerbySetup.initializeTable(
        "INSERT INTO SensorLocations (sensor, room) VALUES (?, ?)",
        IntStream.range(1, 80)
            .mapToObj(i -> new String[] {"sensor_" + i, "room_" + (i % 10)})
            .collect(Collectors.toList())
            .toArray(String[][]::new));

    // start a thread that updates the data of Derby table
    new Thread(
            new DerbySetup.DerbyWriter(
                "UPDATE SensorLocations SET room = ? WHERE sensor = ?",
                random ->
                    new String[] {
                      "room_" + (1 + random.nextInt(20)), "sensor_" + (1 + random.nextInt(80))
                    },
                500))
        .start();

    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000);

    // ingest sensor stream
    DataStream<SensorReading> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner());

    // OPTION 1 (comment out to disable)
    // --------
    // look up the location of a sensor from a Derby table with asynchronous requests.
    DataStream<Tuple2<String, String>> sensorLocations =
        AsyncDataStream.orderedWait(
            readings,
            new DerbyAsyncFunction(),
            5,
            TimeUnit.SECONDS, // timeout requests after 5 seconds
            100); // at most 100 concurrent requests

    // print the sensor locations
    sensorLocations.print();

    env.execute();
  }

  /**
   * AsyncFunction that queries a Derby table via JDBC in a non-blocking fashion.
   *
   * <p>Since the JDBC interface does not support asynchronous queries, starts individual threads to
   * concurrently query Derby and handle the query results in an non-blocking fashion.
   */
  public static class DerbyAsyncFunction
      implements AsyncFunction<SensorReading, Tuple2<String, String>>, Serializable {

    private static final long serialVersionUID = -1892797180196507133L;

    private transient ExecutionContextExecutor cachingPoolExecCtx =
        ExecutionContext.fromExecutor(Executors.newCachedThreadPool());

    // direct execution context to forward result future to callback object
    private transient ExecutionContextExecutor directExcCtx =
        ExecutionContext.fromExecutor(Executors.newCachedThreadPool());

    private void readObject(ObjectInputStream inputStream) {
      cachingPoolExecCtx = ExecutionContext.fromExecutor(Executors.newCachedThreadPool());
      directExcCtx = ExecutionContext.fromExecutor(Executors.newCachedThreadPool());
    }

    /**
     * Executes JDBC query in a thread and handles the resulting Future with an asynchronous
     * callback.
     *
     * @param reading
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(
        SensorReading reading, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {

      String sensor = reading.getId();
      CompletableFuture<String> roomFuture =
          CompletableFuture.supplyAsync(
              () -> {
                // Creating a new connection and statement for each record.
                // Note: This is NOT best practice!
                // Connections and prepared statements should be cached.
                try (Connection connection =
                        DriverManager.getConnection(
                            "jdbc:derby:memory:flinkExample", new Properties());
                    Statement statement = connection.createStatement();
                    // submit query and wait for result. this is a synchronous call.
                    ResultSet resultSet =
                        statement.executeQuery(
                            "SELECT room FROM SensorLocations WHERE sensor = '" + sensor + "'")) {
                  // get room if there is one
                  String room = resultSet.next() ? resultSet.getString(1) : "UNKNOWN ROOM";

                  // sleep to simulate (very) slow requests
                  Thread.sleep(2000);

                  return room;
                } catch (SQLException | InterruptedException ex) {
                  throw new RuntimeException(ex);
                }
              },
              cachingPoolExecCtx);

      roomFuture.whenCompleteAsync(
          (room, throwable) -> {
            if (throwable != null) {
              resultFuture.completeExceptionally(throwable);
            } else {
              resultFuture.complete(Arrays.asList(Tuple2.of(sensor, room)));
            }
          },
          directExcCtx);
    }
  }
}
