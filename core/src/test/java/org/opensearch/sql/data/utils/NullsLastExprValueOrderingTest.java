/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.data.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValue;

class NullsLastExprValueOrderingTest {
  @Test
  public void natural_null_last_null_last() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsLast().nullsLast();
    assertEquals(-1, ordering.compare(integerValue(5), LITERAL_NULL));
    assertEquals(-1, ordering.compare(integerValue(5), LITERAL_MISSING));
  }

  @Test
  public void natural_null_last_null_first() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsLast().nullsFirst();
    assertEquals(1, ordering.compare(integerValue(5), LITERAL_NULL));
    assertEquals(1, ordering.compare(integerValue(5), LITERAL_MISSING));
  }

  @Test
  public void natural_null_last_compare_same_object() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsLast();
    ExprValue exprValue = integerValue(5);
    assertEquals(0, ordering.compare(exprValue, exprValue));
  }
}
