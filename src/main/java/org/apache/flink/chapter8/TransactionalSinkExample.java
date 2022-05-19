package org.apache.flink.chapter8;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.chapter8.util.FailingMapper;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import lombok.SneakyThrows;

/**
 * Example program that demonstrates the behavior of a 2-phase-commit (2PC) sink that writes output
 * to files.
 *
 * <p>The 2PC sink guarantees exactly-once output by writing records immediately to a temp file. For
 * each checkpoint, a new temp file is created. When a checkpoint completes, the corresponding temp
 * file is committed by moving it to a target directory.
 *
 * <p>The program includes a MapFunction that throws an exception in regular intervals to simulate
 * application failures. You can compare the behavior of a 2PC sink and the regular print sink in
 * failure cases.
 *
 * <p>- The TransactionalFileSink commits a file to the target directory when a checkpoint completes
 * and prevents duplicated result output. - The regular print() sink writes to the standard output
 * when a result is produced and duplicates result output in case of a failure.
 */
public class TransactionalSinkExample {
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

    FailingMapper<Tuple2<String, Double>> failingMapper = new FailingMapper<>(16);

    // compute average temperature of all sensors every second
    SingleOutputStreamOperator<Tuple2<String, Double>> avgTempData =
        sensorData
            .timeWindowAll(Time.seconds(1))
            .apply(
                new AllWindowFunction<SensorReading, Tuple2<String, Double>, TimeWindow>() {
                  @Override
                  public void apply(
                      TimeWindow window,
                      Iterable<SensorReading> values,
                      Collector<Tuple2<String, Double>> out)
                      throws Exception {
                    Stream<SensorReading> valuesStream =
                        StreamSupport.stream(values.spliterator(), false);
                    Stream<SensorReading> countStream =
                        StreamSupport.stream(values.spliterator(), false);
                    long count = countStream.count();
                    double avgTemp =
                        valuesStream
                                .map(SensorReading::getTemperature)
                                .mapToDouble(Double::doubleValue)
                                .sum()
                            / count;
                    // format window timestamp as ISO timestamp string
                    long epochSeconds = window.getEnd() / 1000;
                    String timeString =
                        LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC)
                            .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                    // emit record
                    out.collect(Tuple2.of(timeString, avgTemp));
                  }
                })
            // generate failures to trigger job recovery
            .map(failingMapper)
            .setParallelism(1);

    // OPTION 1 (comment out to disable)
    // --------
    // write to files with a transactional sink.
    // results are committed when a checkpoint is completed.
    Tuple2<String, String> paths = createAndGetPaths();
    avgTempData.addSink(new TransactionalFileSink(paths.f0, paths.f1)).setParallelism(1);

    // OPTION 2 (uncomment to enable)
    // --------
    // print to standard out without write-ahead log.
    // results are printed as they are produced and re-emitted in case of a failure.
    //    avgTempData.print()
    //            // enforce sequential writing
    //            .setParallelism(1);

    env.execute();
  }

  /**
   * Creates temporary paths for the output of the transactional file sink.
   *
   * @return
   */
  private static Tuple2<String, String> createAndGetPaths() throws IOException {
    String tempDir = System.getProperty("java.io.tmpdir");
    String targetDir = tempDir + "/committed";
    String transactionDir = tempDir + "/transaction";

    Path targetPath = Paths.get(targetDir);
    Path transactionPath = Paths.get(transactionDir);

    if (!Files.exists(targetPath)) {
      Files.createDirectory(targetPath);
    }

    if (!Files.exists(transactionPath)) {
      Files.createDirectory(transactionPath);
    }

    return Tuple2.of(targetDir, transactionDir);
  }

  /**
   * Transactional sink that writes records to files an commits them to a target directory.
   *
   * <p>Records are written as they are received into a temporary file. For each checkpoint, there
   * is a dedicated file that is committed once the checkpoint (or a later checkpoint) completes.
   */
  private static class TransactionalFileSink
      extends TwoPhaseCommitSinkFunction<Tuple2<String, Double>, String, Void> {

    private final String targetPath;
    private final String tempPath;

    private BufferedWriter transactionWriter;

    public TransactionalFileSink(String targetPath, String tempPath) {
      super(
          Types.STRING.createSerializer(new ExecutionConfig()),
          Types.VOID.createSerializer(new ExecutionConfig()));
      this.targetPath = targetPath;
      this.tempPath = tempPath;
    }

    /**
     * Write record into the current transaction file.
     *
     * @param transaction
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(String transaction, Tuple2<String, Double> value, Context context)
        throws Exception {
      transactionWriter.write(value.toString());
      transactionWriter.write('\n');
    }

    /**
     * Creates a temporary file for a transaction into which the records are written.
     *
     * @return
     * @throws Exception
     */
    @Override
    protected String beginTransaction() throws Exception {
      // path of transaction file is constructed from current time and task index
      String timeNow =
          LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
      int taskIndex = getRuntimeContext().getIndexOfThisSubtask();
      String transactionFile = timeNow + "-" + taskIndex;

      // create transaction file and writer
      Path transactionFilePath = Paths.get(tempPath + "/" + transactionFile);
      Files.createFile(transactionFilePath);
      transactionWriter = Files.newBufferedWriter(transactionFilePath);
      System.out.println("Creating Transaction File: " + transactionFilePath);

      // name of transaction file is returned to later identify the transaction
      return transactionFile;
    }

    /**
     * Flush and close the current transaction file.
     *
     * @param transaction
     * @throws Exception
     */
    @Override
    protected void preCommit(String transaction) throws Exception {
      transactionWriter.flush();
      transactionWriter.close();
    }

    /**
     * Commit a transaction by moving the pre-committed transaction file to the target directory.
     *
     * @param transaction
     */
    @SneakyThrows
    @Override
    protected void commit(String transaction) {
      Path transactionFilePath = Paths.get(tempPath + "/" + transaction);
      // check if the file exists to ensure that the commit is idempotent.
      if (Files.exists(transactionFilePath)) {
        Path commitFilePath = Paths.get(targetPath + "/" + transaction);
        Files.move(transactionFilePath, commitFilePath);
      }
    }

    /**
     * Aborts a transaction by deleting the transaction file.
     *
     * @param transaction
     */
    @SneakyThrows
    @Override
    protected void abort(String transaction) {
      Path transactionFilePath = Paths.get(tempPath + "/" + transaction);
      if (Files.exists(transactionFilePath)) {
        Files.delete(transactionFilePath);
      }
    }
  }
}
