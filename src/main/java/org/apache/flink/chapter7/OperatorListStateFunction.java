package org.apache.flink.chapter7;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class OperatorListStateFunction {
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

    DataStream<Tuple2<Integer, Long>> highTempCounts =
        sensorData.keyBy(SensorReading::getId).flatMap(new HighTempCounterOpState(10.0));

    // print result stream to standard out
    highTempCounts.print();

    // execute application
    env.execute("Count high temperatures");
  }

  /**
   * Counts per parallel instance of the function how many temperature readings exceed the
   * configured threshold.
   */
  private static class HighTempCounterOpState
      extends RichFlatMapFunction<SensorReading, Tuple2<Integer, Long>>
      implements ListCheckpointed<Long> {
    private final double threshold;
    private int subTaskIndex;
    // local count variable
    private long highTempCount = 0L;

    /** @param threshold The high temperature threshold. */
    public HighTempCounterOpState(double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void flatMap(SensorReading reading, Collector<Tuple2<Integer, Long>> out)
        throws Exception {
      if (reading.getTemperature() > threshold) {
        // increment counter if threshold is exceeded
        highTempCount++;
        // emit update with subtask index and counter
        out.collect(Tuple2.of(subTaskIndex, highTempCount));
      }
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
      // restore state by adding all longs of the list
      highTempCount = state.stream().mapToLong(Long::longValue).sum();
    }

    //	  @Override
    //	  public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception
    //	  {
    //      // snapshot state as list with a single count
    //      return Collections.singletonList(highTempCount);
    //	  }

    /** Split count into 10 partial counts for improved state distribution. */
    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
      // split count into ten partial counts
      long div = highTempCount / 10;
      int mod = (int) (highTempCount % 10);
      // return count as ten parts
      return Stream.of(Collections.nCopies(mod, div + 1), Collections.nCopies(10 - mod, div))
          .flatMap(Collection::stream)
          .collect(Collectors.toList());
    }
  }
}
