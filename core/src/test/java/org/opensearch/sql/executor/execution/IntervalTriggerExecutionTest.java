/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

public class IntervalTriggerExecutionTest {

  @Test
  void executeTaskWithInterval() {
    triggerTask(2)
        .taskRun(1)
        .aroundInterval();
  }

  @Test
  void continueExecuteIfTaskRunningLongerThanInterval() {
    triggerTask(1)
        .taskRun(2)
        .aroundTaskRuntime();
  }

  Helper triggerTask(long interval) {
    return new Helper(interval);
  }

  class Helper implements Runnable {

    private StreamingQueryPlan.IntervalTriggerExecution executionStrategy;

    private static final int START = 0;

    private static final int FINISH = 1;

    private static final int UNEXPECTED = 2;

    private int state = START;

    private long interval;

    private long taskExecutionTime;

    Instant start;

    Instant end;

    public Helper(long interval) {
      this.interval = interval;
      this.executionStrategy = new StreamingQueryPlan.IntervalTriggerExecution(interval);
    }

    @SneakyThrows
    Helper taskRun(long taskExecutionTime) {
      this.taskExecutionTime = taskExecutionTime;
      executionStrategy.execute(this::run);
      return this;
    }

    @SneakyThrows
    void aroundInterval() {
      assertTime(interval);
    }

    @SneakyThrows
    void aroundTaskRuntime() {
      assertTime(taskExecutionTime);
    }

    void assertTime(long expected) {
      long took = Duration.between(start, end).toSeconds();
      assertTrue(
          took >= expected - 1 || took <= expected + 1,
          String.format("task interval should around %d, but took :%d", expected, took));
    }

    @SneakyThrows
    @Override
    public void run() {
      assertNotEquals(UNEXPECTED, state);

      if (state == FINISH) {
        end = Instant.now();
        state = UNEXPECTED;
        Thread.currentThread().interrupt();
      } else {
        start = Instant.now();
        state = FINISH;
      }
      TimeUnit.SECONDS.sleep(taskExecutionTime);
    }
  }
}
