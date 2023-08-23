/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;
import org.opensearch.sql.legacy.metrics.GaugeMetric;

public class GaugeMetricTest {

  private static long x = 0;

  @Test
  public void getValue() {
    GaugeMetric gaugeMetric = new GaugeMetric<>("test", this::getSeq);

    assertThat(gaugeMetric.getValue(), equalTo(1L));
    assertThat(gaugeMetric.getValue(), equalTo(2L));
  }

  private long getSeq() {
    return ++x;
  }
}
