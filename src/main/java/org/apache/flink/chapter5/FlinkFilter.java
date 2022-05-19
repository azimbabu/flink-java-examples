package org.apache.flink.chapter5;

import org.apache.flink.api.common.functions.FilterFunction;

public class FlinkFilter implements FilterFunction<String> {
  @Override
  public boolean filter(String value) throws Exception {
    return value.contains("flink");
  }
}
