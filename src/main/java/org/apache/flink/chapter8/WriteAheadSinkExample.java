package org.apache.flink.chapter8;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.apache.flink.streaming.runtime.operators.GenericWriteAheadSink;
import org.apache.flink.util.Collector;

/**
 * Example program that demonstrates the behavior of a write-ahead log sink that prints to the
 * standard output.
 *
 * The write-ahead sink aims prevents duplicate writes the most common failure cases.
 * However, there are failure scenarios in which records may be emitted more than once.
 * The write-ahead sink writes records when a checkpoint completes, i.e., in the configured
 * checkpoint interval.
 *
 * The program includes a MapFunction that throws an exception in regular intervals to simulate
 * application failures.
 * You can compare the behavior of a write-ahead sink and the regular print sink in failure cases.
 *
 * - The StdOutWriteAheadSink writes to the standard output when a checkpoint completes and
 * prevents duplicated result output.
 * - The regular print() sink writes to the standard output when a result is produced and
 * duplicates result output in case of a failure.
 *
 */
public class WriteAheadSinkExample {
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
                    .apply(new AllWindowFunction<SensorReading, Tuple2<String, Double>, TimeWindow>()
                    {
                      @Override
                      public void apply(TimeWindow window, Iterable<SensorReading> values,
                              Collector<Tuple2<String, Double>> out) throws Exception
                      {
                        Stream<SensorReading> valuesStream =
                                StreamSupport.stream(values.spliterator(), false);
                        long count = StreamSupport.stream(values.spliterator(), false).count();
                        double avgTemp =
                                (valuesStream
                                        .map(SensorReading::getTemperature)
                                        .mapToDouble(Double::doubleValue)
                                        .sum())
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
                    .map(value -> failingMapper.map(value))
                    .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                    .setParallelism(1);


    // OPTION 1 (comment out to disable)
    // --------
    // print to standard out with a write-ahead log.
    // results are printed when a checkpoint is completed.
    avgTempData
        .transform(
            "WriteAheadSink", Types.TUPLE(Types.STRING, Types.DOUBLE), new StdOutWriteHeadSink())
        // enforce sequential writing
        .setParallelism(1);

    // OPTION 2 (uncomment to enable)
    // --------
    // print to standard out without write-ahead log.
    // results are printed as they are produced and re-emitted in case of a failure.
//    avgTempData.print()
//            // enforce sequential writing
//            .setParallelism(1);

    env.execute();
  }

  private static class StdOutWriteHeadSink extends GenericWriteAheadSink<Tuple2<String, Double>> {
    public StdOutWriteHeadSink() throws Exception {
      super(
          // CheckpointCommitter that commits checkpoints to the local file system
          new FileCheckpointCommitter(System.getProperty("java.io.tmpdir")),
          // Serializer for records
          Types.<Tuple2<String, Double>>TUPLE(Types.STRING, Types.DOUBLE)
              .createSerializer(new ExecutionConfig()),
          // Random JobID used by the CheckpointCommitter
          UUID.randomUUID().toString());
    }

    @Override
    protected boolean sendValues(
        Iterable<Tuple2<String, Double>> readings, long checkpointId, long timestamp)
        throws Exception {
      for (Tuple2<String, Double> reading : readings) {
        // write record to standard out
        System.out.println(reading);
      }
      return true;
    }

    private static class FileCheckpointCommitter extends CheckpointCommitter {

      private String tempPath;
      private final String basePath;

      public FileCheckpointCommitter(String basePath) {
        this.basePath = basePath;
      }

      @Override
      public void open() throws Exception {
        // no need to open a connection
      }

      @Override
      public void close() throws Exception {
        // no need to close a connection
      }

      @Override
      public void createResource() throws Exception {
        this.tempPath = this.basePath + "/" + this.jobId;

        // create directory for commit file
        Files.createDirectory(Paths.get(tempPath));
      }

      @Override
      public void commitCheckpoint(int subtaskIdx, long checkpointID) throws Exception {
        Path commitPath = Paths.get(tempPath + "/" + subtaskIdx);
        // convert checkpointID to hexString
        String hexID = "0x" + StringUtils.leftPad(Long.toHexString(checkpointID), 16, "0");
        // write hexString to commit file
        Files.write(commitPath, hexID.getBytes());
      }

      @Override
      public boolean isCheckpointCommitted(int subtaskIdx, long checkpointID) throws Exception {
        Path commitPath = Paths.get(tempPath + "/" + subtaskIdx);
        if (!Files.exists(commitPath)) {
          return false;
        } else {
          // read committed checkpoint id from commit file
          String hexID = Files.readAllLines(commitPath).get(0);
          Long checkpointed = Long.decode(hexID);
          // check if committed id is less or equal to requested id
          return checkpointID <= checkpointed;
        }
      }
    }
  }
}
