/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;

import org.junit.jupiter.api.Test;

class ReverseExprValueOrderingTest {
  @Test
  public void natural_reverse_reverse() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().reverse().reverse();
    assertEquals(1, ordering.compare(integerValue(5), integerValue(4)));
    assertEquals(0, ordering.compare(integerValue(5), integerValue(5)));
    assertEquals(-1, ordering.compare(integerValue(4), integerValue(5)));
  }
}
