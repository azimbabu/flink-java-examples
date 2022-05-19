package org.apache.flink.chapter8;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.common.SensorSource;
import org.apache.flink.common.SensorTimeAssigner;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;

public class CassandraTupleSink {
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
    DataStream<Tuple2<String, Double>> readings =
        env
            // SensorSource generates random temperature readings
            .addSource(new SensorSource())
            // assign timestamps and watermarks which are required for event time
            .assignTimestampsAndWatermarks(new SensorTimeAssigner())
            .map(r -> Tuple2.of(r.getId(), r.getTemperature()))
            .returns(
                new TupleTypeInfo<>(
                    TypeInformation.of(String.class), TypeInformation.of(Double.class)));

    // Example 8-7. Creating a Cassandra sink for tuples
    CassandraSink.CassandraSinkBuilder<Tuple2<String, Double>> sinkBuilder =
        CassandraSink.addSink(readings);
    sinkBuilder
        .setHost("localhost")
        .setQuery("INSERT INTO flink_example.sensors(sensorId, temperature) VALUES (?, ?);")
        .build();

    // execute application
    env.execute("Generate Temperature Alerts");
  }
}
