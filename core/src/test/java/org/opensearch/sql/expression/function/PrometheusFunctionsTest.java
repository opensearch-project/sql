package org.opensearch.sql.expression.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.STRUCT;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

public class PrometheusFunctionsTest extends ExpressionTestBase {

  @Test
  public void query_range() {
    FunctionExpression expr = dsl.query_range_function(
        dsl.namedArgument("query", DSL.literal("http_latency")),
        dsl.namedArgument("starttime", DSL.literal(12345)),
        dsl.namedArgument("endtime", DSL.literal(12345)),
        dsl.namedArgument("step", DSL.literal(14)));
    assertEquals(STRUCT, expr.type());
    assertEquals("query_range(query=\"http_latency\", starttime=12345, endtime=12345, step=14)",
        expr.toString());
  }

  @Test
  public void query_range_position_parameters() {
    FunctionExpression expr = dsl.query_range_function(
        dsl.namedArgument("", DSL.literal("http_latency")),
        dsl.namedArgument("", DSL.literal(12345)),
        dsl.namedArgument("", DSL.literal(12345)),
        dsl.namedArgument("", DSL.literal(14)));
    assertEquals(STRUCT, expr.type());
    assertEquals("query_range(query=\"http_latency\", starttime=12345, endtime=12345, step=14)",
        expr.toString());
  }

  @Test
  public void query_range_value_of() {
    FunctionExpression expr = dsl.query_range_function(
        dsl.namedArgument("query", DSL.literal("http_latency")),
        dsl.namedArgument("starttime", DSL.literal(12345)),
        dsl.namedArgument("endtime", DSL.literal(12345)),
        dsl.namedArgument("step", DSL.literal(14)));
    assertThrows(UnsupportedOperationException.class,
        () -> expr.valueOf(valueEnv()),
        "Prometheus defined function [query_range] is "
            + "only supported in SOURCE clause with prometheus catalog");
  }

}
