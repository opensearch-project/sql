/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.bindingtuple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;

class BindingTupleTest {
  @Test
  public void resolve_ref_expression() {
    BindingTuple bindingTuple =
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "209.160.24.63")).bindingTuples();
    assertEquals(
        ExprValueUtils.stringValue("209.160.24.63"), bindingTuple.resolve(DSL.ref("ip", STRING)));
  }

  @Test
  public void resolve_missing_expression() {
    BindingTuple bindingTuple =
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "209.160.24.63")).bindingTuples();
    assertEquals(
        ExprValueUtils.LITERAL_MISSING, bindingTuple.resolve(DSL.ref("ip_missing", STRING)));
  }

  @Test
  public void resolve_from_empty_tuple() {
    assertEquals(
        ExprValueUtils.LITERAL_MISSING, BindingTuple.EMPTY.resolve(DSL.ref("ip_missing", STRING)));
  }

  @Test
  public void resolve_literal_expression_throw_exception() {
    BindingTuple bindingTuple =
        ExprValueUtils.tupleValue(ImmutableMap.of("ip", "209.160.24.63")).bindingTuples();
    ExpressionEvaluationException exception =
        assertThrows(
            ExpressionEvaluationException.class, () -> bindingTuple.resolve(DSL.literal(1)));
    assertEquals("can resolve expression: 1", exception.getMessage());
  }
}
