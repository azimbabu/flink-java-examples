package org.apache.flink.chapter5;

import java.util.Random;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/** Flink SourceFunction to generate random SmokeLevel events. */
public class SmokeLevelSource implements SourceFunction<SmokeLevel> {
  // flag indicating whether source is still running
  private boolean running = true;

  /**
   * Continuously emit one smoke level event per second.
   *
   * @param ctx
   * @throws Exception
   */
  @Override
  public void run(SourceContext<SmokeLevel> ctx) throws Exception {
    // initialize random number generator
    Random random = new Random();

    while (running) {
      if (random.nextGaussian() > 0.8) {
        // emit a high SmokeLevel
        ctx.collect(SmokeLevel.HIGH);
      } else {
        ctx.collect(SmokeLevel.LOW);
      }

      // wait for 1 second
      Thread.sleep(1000);
    }
  }

  /** Cancel the emission of smoke level events. */
  @Override
  public void cancel() {
    running = false;
  }
}
