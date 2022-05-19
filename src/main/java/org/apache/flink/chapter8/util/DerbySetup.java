package org.apache.flink.chapter8.util;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

/** Methods to setup an embedded, in-memory Derby database. */
public class DerbySetup {
  /**
   * Sets up an embedded in-memory Derby database and creates the table.
   *
   * @param tableDDL
   */
  public static void setupDerby(String tableDDL)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {
    // start embedded in-memory Derby and create a connection
    Class.forName(org.apache.derby.jdbc.EmbeddedDriver.class.getName()).newInstance();
    try (Connection connection =
            DriverManager.getConnection(
                "jdbc:derby:memory:flinkExample;create=true", new Properties());
        Statement statement = connection.createStatement()) {
      // create the table to which the sink writes
      statement.execute(tableDDL);
    }
  }

  public static void initializeTable(String statement, Object[][] params) throws SQLException {
    // connect to embedded in-memory Derby and prepare query
    try (Connection connection =
            DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties());
        PreparedStatement preparedStatement = connection.prepareStatement(statement)) {

      for (Object[] statementParams : params) {
        for (int i = 1; i <= statementParams.length; i++) {
          preparedStatement.setObject(i, statementParams[i - 1]);
        }
        // update the derby table
        preparedStatement.addBatch();
      }
      preparedStatement.executeBatch();
    }
  }

  /** A Runnable that queries the Derby table in intervals and prints the result. */
  public static class DerbyReader implements Runnable, AutoCloseable {
    private final Connection connection;
    private final PreparedStatement preparedStatement;
    private final int numResultCols;
    private final String query;
    private final long intervals;

    public DerbyReader(String query, long intervals) throws SQLException {
      this.query = query;
      this.intervals = intervals;

      // connect to embedded in-memory Derby and prepare query
      connection = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties());
      preparedStatement = connection.prepareStatement(query);
      numResultCols = preparedStatement.getMetaData().getColumnCount();
    }

    @SneakyThrows
    @Override
    public void run() {
      Object[] values = new Object[numResultCols];
      while (true) {
        // wait for the interval
        Thread.sleep(intervals);
        // query the Derby table and print the result
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          while (resultSet.next()) {
            for (int i = 1; i <= numResultCols; i++) {
              values[i - 1] = resultSet.getObject(i);
            }
            List<String> printableValues =
                Arrays.stream(values).map(Object::toString).collect(Collectors.toList());
            System.out.println(String.join(", ", printableValues));
          }
        }
      }
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  /** A Runnable that writes in intervals to a Derby table. */
  public static class DerbyWriter implements Runnable, AutoCloseable {

    private final Function<Random, Object[]> paramGenerator;
    private final long interval;
    private final Connection connection;
    private final PreparedStatement preparedStatement;
    private final Random random;

    public DerbyWriter(String statement, Function<Random, Object[]> paramGenerator, long interval)
        throws SQLException {
      this.paramGenerator = paramGenerator;
      this.interval = interval;
      random = new Random(1234);

      // connect to embedded in-memory Derby and prepare query
      connection = DriverManager.getConnection("jdbc:derby:memory:flinkExample", new Properties());
      preparedStatement = connection.prepareStatement(statement);
    }

    @SneakyThrows
    @Override
    public void run() {
      while (true) {
        Thread.sleep(interval);
        // get and set parameters
        Object[] params = paramGenerator.apply(random);
        for (int i = 1; i <= params.length; i++) {
          preparedStatement.setObject(i, params[i - 1]);
        }
        // update the Derby table
        preparedStatement.executeUpdate();
      }
    }

    @SneakyThrows
    @Override
    public void close() throws IOException {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }
}
