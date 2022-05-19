package org.apache.flink.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** POJO to hold sensor reading data. */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SensorReading {
  // id of the sensor
  private String id;
  // timestamp of the reading
  private long timestamp;
  // temperature value of the reading
  private double temperature;

  @Override
  public String toString() {
    return "(" + id + ", " + timestamp + ", " + temperature + ")";
  }
}
