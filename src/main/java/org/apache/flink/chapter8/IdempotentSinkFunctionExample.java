package org.apache.flink.chapter8;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import org.apache.flink.chapter8.util.DerbySetup;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * Example program that emits sensor readings with UPSERT writes to an embedded in-memory Apache
 * Derby database.
 *
 * <p>A separate thread queries the database every 10 seconds and prints the result.
 */
public class IdempotentSinkFunctionExample {
  public static void main(String[] args) throws Exception {
    // setup the embedded Derby database
    DerbySetup.setupDerby(
        "CREATE TABLE Temperatures (sensor VARCHAR(16) PRIMARY KEY, temp DOUBLE)");
    // start a thread that prints the data written to Derby every 10 seconds.
    try (DerbySetup.DerbyReader reader =
        new DerbySetup.DerbyReader(
            "SELECT sensor, temp FROM Temperatures ORDER BY sensor", 10000)) {
      new Thread(reader).start();

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

      SingleOutputStreamOperator<SensorReading> celsiusReadings = sensorData.map(
              r -> new SensorReading(r.getId(), r.getTimestamp(),
                      (r.getTemperature() - 32) * (5.0 / 9.0)));

      // write the converted sensor readings to Derby.
      celsiusReadings.addSink(new DerbyUpsertSink());

      env.execute();
    }
  }

  /**
   * Sink that upserts SensorReadings into a Derby table that is keyed on the sensor id.
   *
   * <p>Since Derby does not feature a dedicated UPSERT command, we execute an UPDATE statement
   * first and execute an INSERT statement if UPDATE did not modify any row.
   */
  private static class DerbyUpsertSink extends RichSinkFunction<SensorReading> {
    private Connection connection;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;

    @Override
    public void open(Configuration parameters) throws Exception {
      // connect to embedded in-memory Derby
      connection = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties());
      // prepare insert and update statements
      insertStmt =
          connection.prepareStatement("INSERT INTO Temperatures (sensor, temp) VALUES (?, ?)");
      updateStmt = connection.prepareStatement("UPDATE Temperatures SET temp = ? WHERE sensor = ?");
    }

    @Override
    public void close() throws Exception {
      insertStmt.close();
      updateStmt.close();
      connection.close();
    }

    @Override
    public void invoke(SensorReading reading, Context context) throws Exception {
      // set parameters for update statement and execute it
      updateStmt.setDouble(1, reading.getTemperature());
      updateStmt.setString(2, reading.getId());
      updateStmt.execute();
      // execute insert statement if update statement did not update any row
      if (updateStmt.getUpdateCount() == 0) {
        // set parameters for insert statement
        insertStmt.setString(1, reading.getId());
        insertStmt.setDouble(2, reading.getTemperature());
        // execute insert statement
        insertStmt.execute();
      }
    }
  }
}
