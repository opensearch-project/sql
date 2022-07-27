/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.HighlightExpression;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.env.Environment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

public class HighlightExpressionTest extends ExpressionTestBase {

  @Test
  public void single_highlight_test() {
    Environment<Expression, ExprValue> tmp = ExprValueUtils.tupleValue(
        ImmutableMap.of("_highlight.Title", "result value")).bindingTuples();
    HighlightExpression expr = new HighlightExpression(DSL.literal("Title"));
    ExprValue resultVal = expr.valueOf(tmp);

    assertEquals(STRING, resultVal.type());
    assertEquals("result value", resultVal.stringValue());
  }

  @Test
  public void highlight_all_test() {
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
    var hlBuilder = ImmutableMap.<String, ExprValue>builder();
    hlBuilder.put("Title", ExprValueUtils.stringValue("correct result value"));
    hlBuilder.put("Body", ExprValueUtils.stringValue("incorrect result value"));
    builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));

    HighlightExpression expr = new HighlightExpression(DSL.literal("*"));
    ExprValue resultVal = expr.valueOf(ExprTupleValue.fromExprValueMap(builder.build()).bindingTuples());
    assertEquals(STRUCT, resultVal.type());
    assertTrue(resultVal.tupleValue().containsValue(ExprValueUtils.stringValue("\"correct result value\"")));
    assertTrue(resultVal.tupleValue().containsValue(ExprValueUtils.stringValue("\"correct result value\"")));
  }
}
