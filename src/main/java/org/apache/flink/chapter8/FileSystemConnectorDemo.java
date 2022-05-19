package org.apache.flink.chapter8;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

public class FileSystemConnectorDemo {
  public static void main(String[] args) {
    // set up the streaming execution environment
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    // checkpoint every 10 seconds
    env.getCheckpointConfig().setCheckpointInterval(10 * 1000);
    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    // configure watermark interval
    env.getConfig().setAutoWatermarkInterval(1000);

    TextInputFormat lineReader = new TextInputFormat(null);
    DataStream<String> input =
        env.readFile(
            lineReader, // The FileInputFormat
            "hdfs:///path/to/my/data", // The path to read
            FileProcessingMode.PROCESS_CONTINUOUSLY, // The processing mode
            30000L // The monitoring interval in ms
            );

    // Creating a StreamingFileSink in row-encoding mode
    StreamingFileSink<String> sink =
        StreamingFileSink.forRowFormat(
                new Path("/base/path"), new SimpleStringEncoder<String>("UTF-8"))
            .build();
    input.addSink(sink);

    // Creating a StreamingFileSink in bulk-encoding mode
    StreamingFileSink<AvroPojo> bulkEncodingSink =
        StreamingFileSink.forBulkFormat(
                new Path("/base/path"), ParquetAvroWriters.forSpecificRecord(AvroPojo.class))
            .build();
  }

  public static class AvroPojo extends SpecificRecordBase {

    @Override
    public Schema getSchema() {
      return null;
    }

    @Override
    public Object get(int field) {
      return null;
    }

    @Override
    public void put(int field, Object value) {}
  }
}
