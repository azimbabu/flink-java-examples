package org.apache.flink.chapter8;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Table(keyspace = "flink_example", name = "sensors")
public class SensorReadings {
  @Column(name = "sensorId")
  private String id;

  @Column(name = "temperature")
  private Double temperature;
}
