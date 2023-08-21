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

class NullsFirstExprValueOrderingTest {
  @Test
  public void natural_null_first_null_first() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsFirst().nullsFirst();
    assertEquals(1, ordering.compare(integerValue(5), LITERAL_NULL));
    assertEquals(1, ordering.compare(integerValue(5), LITERAL_MISSING));
  }

  @Test
  public void natural_null_first_null_Last() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsFirst().nullsLast();
    assertEquals(-1, ordering.compare(integerValue(5), LITERAL_NULL));
    assertEquals(-1, ordering.compare(integerValue(5), LITERAL_MISSING));
  }

  @Test
  public void natural_null_first_compare_same_object() {
    ExprValueOrdering ordering = ExprValueOrdering.natural().nullsFirst();
    ExprValue exprValue = integerValue(5);
    assertEquals(0, ordering.compare(exprValue, exprValue));
  }
}
