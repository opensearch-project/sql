/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;



@ExtendWith(MockitoExtension.class)
class ConvertTZTest extends ExpressionTestBase {

  @Mock
  Environment<Expression, ExprValue> env;

  @Mock
  Expression nullRef;

  @Mock
  Expression missingRef;

  @BeforeEach
  public void setup() {
    when(nullRef.valueOf(env)).thenReturn(nullValue());
    when(missingRef.valueOf(env)).thenReturn(missingValue());
  }

  @Test
  public void convert_tz() {
    when(nullRef.type()).thenReturn(DATETIME);
    when(missingRef.type()).thenReturn(DATETIME);
    assertEquals(nullValue(), eval(dsl.date(nullRef)));
    assertEquals(missingValue(), eval(dsl.date(missingRef)));

    FunctionExpression expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+10:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-16 08:00:00"), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+16:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("-16:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(DSL.literal("2008-05-15 22:00:00"),
        DSL.literal("+00:00"),
        DSL.literal("-16:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+15:00"),
        DSL.literal("+01:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("-15:00"),
        DSL.literal("+01:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("-12:00"),
        DSL.literal("+15:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+14:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-16 12:00:00"), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:01"),
        DSL.literal("-13:59"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-15 08:00:00"), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+)()"),
        DSL.literal("+12:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
        DSL.literal("2008-05-15 22:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("test"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(
        DSL.literal("test"),
        DSL.literal("+00:00"),
        DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
            DSL.literal("2021-02-30 10:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
            DSL.literal("2021-04-31 10:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));

    expr = dsl.convert_tz(dsl.datetime(
            DSL.literal("2021-13-03 10:00:00")),
        DSL.literal("+00:00"),
        DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
