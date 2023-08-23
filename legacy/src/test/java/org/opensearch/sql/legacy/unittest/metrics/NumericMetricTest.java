/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;
import org.opensearch.sql.legacy.metrics.BasicCounter;
import org.opensearch.sql.legacy.metrics.NumericMetric;

public class NumericMetricTest {

  @Test
  public void increment() {
    NumericMetric metric = new NumericMetric("test", new BasicCounter());
    for (int i = 0; i < 5; ++i) {
      metric.increment();
    }

    assertThat(metric.getValue(), equalTo(5L));
  }

  @Test
  public void add() {
    NumericMetric metric = new NumericMetric("test", new BasicCounter());
    metric.increment(5);

    assertThat(metric.getValue(), equalTo(5L));
  }
}
