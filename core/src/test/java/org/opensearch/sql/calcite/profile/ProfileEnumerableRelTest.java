/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.monitor.profile.ProfilePlanNodeMetrics;

class ProfileEnumerableRelTest {

  @Test
  void profileReturnsSameEnumerableWhenMetricsMissing() {
    Enumerable<Integer> enumerable = new TestEnumerable(2);

    Enumerable<Integer> result = ProfileEnumerableRel.profile(enumerable, null);

    assertSame(enumerable, result);
  }

  @Test
  void profileTracksRowsAndTime() {
    ProfilePlanNodeMetrics metrics = new ProfilePlanNodeMetrics();
    Enumerable<Integer> enumerable = new TestEnumerable(2);

    Enumerator<Integer> enumerator = ProfileEnumerableRel.profile(enumerable, metrics).enumerator();
    while (enumerator.moveNext()) {
      enumerator.current();
    }
    enumerator.close();

    assertEquals(2, metrics.rows());
    assertTrue(metrics.timeNanos() > 0);
  }

  private static final class TestEnumerable extends AbstractEnumerable<Integer> {
    private final int size;

    private TestEnumerable(int size) {
      this.size = size;
    }

    @Override
    public Enumerator<Integer> enumerator() {
      return new Enumerator<>() {
        private int index = -1;

        @Override
        public Integer current() {
          return index;
        }

        @Override
        public boolean moveNext() {
          sleepMillis(1);
          index++;
          return index < size;
        }

        @Override
        public void reset() {
          index = -1;
        }

        @Override
        public void close() {
          sleepMillis(1);
        }
      };
    }
  }

  private static void sleepMillis(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
