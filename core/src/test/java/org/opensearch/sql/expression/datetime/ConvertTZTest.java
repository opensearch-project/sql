/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class ConvertTZTest extends ExpressionTestBase {

  @Test
  public void invalidDate() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2021-04-31 10:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void conversionFromNoOffset() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+10:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-16 08:00:00"), expr.valueOf());
  }

  @Test
  public void conversionToInvalidInput3Over() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+16:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void conversionToInvalidInput3Under() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("-16:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void conversionFromPositiveToPositive() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+15:00"),
            DSL.literal("+01:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidInput2Under() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("-15:00"),
            DSL.literal("+01:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidInput3Over() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("-12:00"),
            DSL.literal("+15:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void conversionToPositiveEdge() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+14:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-16 12:00:00"), expr.valueOf());
  }

  @Test
  public void conversionToNegativeEdge() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:01"),
            DSL.literal("-13:59"));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2008-05-15 08:00:00"), expr.valueOf());
  }

  @Test
  public void invalidInput2() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+)()"),
            DSL.literal("+12:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidInput3() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2008-05-15 22:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("test"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidInput1() {
    FunctionExpression expr =
        DSL.convert_tz(DSL.literal("test"), DSL.literal("+00:00"), DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidDateFeb30() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2021-02-30 10:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidDateApril31() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2021-04-31 10:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidMonth13() {
    FunctionExpression expr =
        DSL.convert_tz(
            DSL.datetime(DSL.literal("2021-13-03 10:00:00")),
            DSL.literal("+00:00"),
            DSL.literal("+00:00"));
    assertEquals(DATETIME, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }
}
