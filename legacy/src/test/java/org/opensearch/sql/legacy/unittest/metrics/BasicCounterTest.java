/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Test;
import org.opensearch.sql.legacy.metrics.BasicCounter;

public class BasicCounterTest {

  @Test
  public void increment() {
    BasicCounter counter = new BasicCounter();
    for (int i = 0; i < 5; ++i) {
      counter.increment();
    }

    assertThat(counter.getValue(), equalTo(5L));
  }

  @Test
  public void incrementN() {
    BasicCounter counter = new BasicCounter();
    counter.add(5);

    assertThat(counter.getValue(), equalTo(5L));
  }
}
