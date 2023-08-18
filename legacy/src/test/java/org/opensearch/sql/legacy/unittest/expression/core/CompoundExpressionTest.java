/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.expression.core;

import static org.junit.Assert.assertEquals;
import static org.opensearch.sql.legacy.expression.core.ExpressionFactory.literal;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.doubleValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueFactory.integerValue;

import org.junit.Test;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperation;

public class CompoundExpressionTest extends ExpressionTest {

  @Test
  public void absAndAddShouldPass() {
    assertEquals(
        2.0d,
        apply(
            ScalarOperation.ABS,
            of(ScalarOperation.ADD, literal(doubleValue(-1.0d)), literal(integerValue(-1)))));
  }
}
