package org.apache.flink.chapter5;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** POJO representing an alert. */
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Alert {
  private String message;
  private long timestamp;

  @Override
  public String toString() {
    return "(" + message + ", " + timestamp + ")";
  }
}
