/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.ARRAY;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.expression.env.Environment;


public class HighlightExpressionTest extends ExpressionTestBase {

  @Test
  public void single_highlight_test() {
    Environment<Expression, ExprValue> hlTuple = ExprValueUtils.tupleValue(
        ImmutableMap.of("_highlight.Title", "result value")).bindingTuples();
    HighlightExpression expr = new HighlightExpression(DSL.literal("Title"));
    ExprValue resultVal = expr.valueOf(hlTuple);

    assertEquals(expr.type(), ARRAY);
    assertEquals("result value", resultVal.stringValue());
  }

  @Test
  public void missing_highlight_test() {
    Environment<Expression, ExprValue> hlTuple = ExprValueUtils.tupleValue(
        ImmutableMap.of("_highlight.Title", "result value")).bindingTuples();

    HighlightExpression expr = new HighlightExpression(DSL.literal("invalid"));
    ExprValue resultVal = expr.valueOf(hlTuple);

    assertTrue(resultVal.isMissing());
  }

  @Test
  public void missing_highlight_wildcard_test() {
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
    var hlBuilder = ImmutableMap.<String, ExprValue>builder();
    hlBuilder.put("Title", ExprValueUtils.stringValue("first result value"));
    hlBuilder.put("Body", ExprValueUtils.stringValue("secondary result value"));
    builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));

    HighlightExpression hlExpr = new HighlightExpression(DSL.literal("invalid*"));
    ExprValue resultVal = hlExpr.valueOf(
        ExprTupleValue.fromExprValueMap(builder.build()).bindingTuples());

    assertTrue(resultVal.isMissing());
  }

  @Test
  public void highlight_all_test() {
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
    var hlBuilder = ImmutableMap.<String, ExprValue>builder();
    hlBuilder.put("Title", ExprValueUtils.stringValue("correct result value"));
    hlBuilder.put("Body", ExprValueUtils.stringValue("secondary correct result value"));
    builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));

    HighlightExpression hlExpr = new HighlightExpression(DSL.literal("T*"));
    ExprValue resultVal = hlExpr.valueOf(
        ExprTupleValue.fromExprValueMap(builder.build()).bindingTuples());

    assertEquals(STRUCT, resultVal.type());
    assertTrue(resultVal.tupleValue().containsValue(
        ExprValueUtils.stringValue("correct result value")));
    assertFalse(resultVal.tupleValue().containsValue(
        ExprValueUtils.stringValue("secondary correct result value")));
  }

  @Test
  public void do_nothing_with_missing_value() {
    Environment<Expression, ExprValue> hlTuple = ExprValueUtils.tupleValue(
        ImmutableMap.of("NonHighlightField", "ResultValue")).bindingTuples();
    HighlightExpression expr = new HighlightExpression(DSL.literal("*"));
    ExprValue resultVal = expr.valueOf(hlTuple);

    assertTrue(resultVal.isMissing());
  }

  @Test
  public void highlight_wildcard_test() {
    ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
    var hlBuilder = ImmutableMap.<String, ExprValue>builder();
    hlBuilder.put("Title", ExprValueUtils.stringValue("correct result value"));
    hlBuilder.put("Body", ExprValueUtils.stringValue("incorrect result value"));
    builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));

    HighlightExpression hlExpr = new HighlightExpression(DSL.literal("T*"));
    ExprValue resultVal = hlExpr.valueOf(
        ExprTupleValue.fromExprValueMap(builder.build()).bindingTuples());

    assertEquals(STRUCT, resultVal.type());
    assertTrue(resultVal.tupleValue().containsValue(
        ExprValueUtils.stringValue("correct result value")));
    assertFalse(resultVal.tupleValue().containsValue(
        ExprValueUtils.stringValue("incorrect result value")));
  }
}
