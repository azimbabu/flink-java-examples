package org.apache.flink.chapter8;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class KafkaConnectorDemo {
  public static void main(String[] args) {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000);

    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    properties.setProperty("group.id", "test");

    DataStream<String> stream =
        env.addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));

    FlinkKafkaProducer<String> myProducer =
        new FlinkKafkaProducer<String>(
            "localhost:9092", // broker list
            "topic", // target topic
            new SimpleStringSchema() // serialization schema
            );
    stream.addSink(myProducer);
  }
}
