package org.apache.flink.chapter8;

import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;

import org.apache.flink.common.SensorReading;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class SinkFunctionExample
{
  public static void main(String[] args) throws Exception
  {
	  StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	  // checkpoint every 10 seconds
	  env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
	  // use event time for the application
	  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	  // configure watermark interval
	  env.getConfig().setAutoWatermarkInterval(1000);

	  // ingest sensor stream
	  DataStream<SensorReading> readings = env
			  // SensorSource generates random temperature readings
			  .addSource(new SensorSource())
			  // assign timestamps and watermarks which are required for event time
			  .assignTimestampsAndWatermarks(new SensorTimeAssigner());

	  // write the sensor readings to a socket
	  readings.addSink(new SimpleSocketSink("localhost", 9191))
			  // set parallelism to 1 because only one thread can write to a socket
			  .setParallelism(1);

	  env.execute();
  }

  private static class SimpleSocketSink extends RichSinkFunction<SensorReading>
  {
	  private final String host;
	  private final int port;
	  private Socket socket;
	  private PrintStream writer;

	  public SimpleSocketSink(String host, int port) {
		  this.host = host;
		  this.port = port;
	  }

	  @Override
	  public void open(Configuration parameters) throws Exception
	  {
		  // open socket and writer
		  socket = new Socket(InetAddress.getByName(host), port);
		  writer = new PrintStream(socket.getOutputStream());
	  }

	  @Override
	  public void close() throws Exception
	  {
		  // close writer and socket
		  if (writer != null) {
			  writer.close();
		  }
		  if (socket != null) {
			  socket.close();
		  }
	  }

	  @Override
	  public void invoke(SensorReading value, Context context) throws Exception
	  {
		  // write sensor reading to socket
		  writer.println(value.toString());
		  writer.flush();
	  }
  }
}
