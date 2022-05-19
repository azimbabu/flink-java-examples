package org.apache.flink.chapter7;

import java.util.stream.StreamSupport;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckpointedFunctionExample {
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

    SingleOutputStreamOperator<Tuple3<String, Long, Long>> highTempCounts =
        sensorData.keyBy(SensorReading::getId).flatMap(new HighTempCounter(10.0));

    highTempCounts.print();
    env.execute();
  }

  private static class HighTempCounter
      implements FlatMapFunction<SensorReading, Tuple3<String, Long, Long>>, CheckpointedFunction {
    private final double threshold;
    // local variable for the operator high temperature cnt
    private long opHighTempCount = 0;

    private ValueState<Long> keyedCountState;
    private ListState<Long> opCountState;

    public HighTempCounter(double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void flatMap(SensorReading reading, Collector<Tuple3<String, Long, Long>> out)
        throws Exception {
      // check if temperature is high
      if (reading.getTemperature() > threshold) {
        // update local operator high temp counter
        opHighTempCount++;
        // update keyed high temp counter
        Long keyHighTempCount = keyedCountState.value();
        if (keyHighTempCount == null) {
          keyHighTempCount = 0L;
        }
        keyedCountState.update(keyHighTempCount + 1);
        // emit new counters
        out.collect(Tuple3.of(reading.getId(), keyHighTempCount, opHighTempCount));
      }
    }

    @Override
    public void initializeState(FunctionInitializationContext initContext) throws Exception {
      // initialize keyed state
      ValueStateDescriptor<Long> keyCountDescriptor =
          new ValueStateDescriptor<>("keyedCount", Long.class);
      keyedCountState = initContext.getKeyedStateStore().getState(keyCountDescriptor);

      // initialize operator state
      ListStateDescriptor<Long> opCountDescriptor =
          new ListStateDescriptor<Long>("opCount", Long.class);
      opCountState = initContext.getOperatorStateStore().getListState(opCountDescriptor);

      // initialize local variable with state
      opHighTempCount =
          StreamSupport.stream(opCountState.get().spliterator(), false)
              .mapToLong(Long::longValue)
              .sum();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext snapshotContext) throws Exception {
      // update operator state with local state
      opCountState.clear();
      opCountState.add(opHighTempCount);
    }
  }
}
