package org.apache.flink.chapter8;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SourceFunctionExample
{
  public static void main(String[] args) throws Exception
  {
	  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//	  DataStream<Long> numbers = env.addSource(new CountSource());
	  DataStream<Long> numbers = env.addSource(new ReplayableCountSource());
	  numbers.print();

	  env.execute();
  }

  private static class CountSource implements SourceFunction<Long>
  {
	  private boolean isRunning = true;

	  @Override
	  public void run(SourceContext<Long> ctx) throws Exception
	  {
		  long count = -1;
		  while (isRunning && count < Long.MAX_VALUE) {
			  count++;
			  ctx.collect(count);
		  }
	  }

	  @Override
	  public void cancel()
	  {
		  isRunning = false;
	  }
  }

  private static class ReplayableCountSource implements SourceFunction<Long>, CheckpointedFunction {
	  private boolean isRunning = true;
	  private ListState<Long> offsetState;
	  private long count;

	  @Override
	  public void snapshotState(FunctionSnapshotContext context) throws Exception
	  {
		  // remove previous count
		  offsetState.clear();
		  // add current count
		  offsetState.add(count);
	  }

	  @Override
	  public void initializeState(FunctionInitializationContext initCtx) throws Exception
	  {
		  ListStateDescriptor<Long> desc = new ListStateDescriptor<Long>("offset", Long.class);
		  offsetState = initCtx.getOperatorStateStore().getListState(desc);
		  // initialize count variable
		  Iterable<Long> it = offsetState.get();
		  count = (it == null || !it.iterator().hasNext()) ? -1 : it.iterator().next();
	  }

	  @Override
	  public void run(SourceContext<Long> ctx) throws Exception
	  {
		  while (isRunning && count < Long.MAX_VALUE) {
			  // synchronize data emission and checkpoints
			  synchronized (ctx.getCheckpointLock()) {
				  count++;
				  ctx.collect(count);
			  }
		  }
	  }

	  @Override
	  public void cancel()
	  {
		  isRunning = false;
	  }
  }
}
