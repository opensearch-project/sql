/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.intervalValue;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;

import java.time.Duration;
import java.time.Period;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
public class IntervalClauseTest extends ExpressionTestBase {
  @Mock
  Environment<Expression, ExprValue> env;

  @Mock
  Expression nullRef;

  @Mock
  Expression missingRef;

  @Test
  public void microsecond() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("microsecond"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Duration.ofNanos(1000)), expr.valueOf(env));
  }

  @Test
  public void second() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("second"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Duration.ofSeconds(1)), expr.valueOf(env));
  }

  @Test
  public void minute() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("minute"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Duration.ofMinutes(1)), expr.valueOf(env));
  }

  @Test
  public void hour() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("HOUR"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Duration.ofHours(1)), expr.valueOf(env));
  }

  @Test
  public void day() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("day"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Duration.ofDays(1)), expr.valueOf(env));
  }

  @Test
  public void week() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("week"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Period.ofWeeks(1)), expr.valueOf(env));
  }

  @Test
  public void month() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("month"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Period.ofMonths(1)), expr.valueOf(env));
  }

  @Test
  public void quarter() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("quarter"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Period.ofMonths(3)), expr.valueOf(env));
  }

  @Test
  public void year() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("year"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(intervalValue(Period.ofYears(1)), expr.valueOf(env));
  }

  @Test
  public void unsupported_unit() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("year_month"));
    assertThrows(ExpressionEvaluationException.class, () -> expr.valueOf(env),
        "interval unit year_month is not supported");
  }

  @Test
  public void to_string() {
    FunctionExpression expr = dsl.interval(DSL.literal(1), DSL.literal("day"));
    assertEquals("interval(1, \"day\")", expr.toString());
  }

  @Test
  public void null_value() {
    when(nullRef.type()).thenReturn(INTEGER);
    when(nullRef.valueOf(env)).thenReturn(nullValue());
    FunctionExpression expr = dsl.interval(nullRef, DSL.literal("day"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(nullValue(), expr.valueOf(env));
  }

  @Test
  public void missing_value() {
    when(missingRef.type()).thenReturn(INTEGER);
    when(missingRef.valueOf(env)).thenReturn(missingValue());
    FunctionExpression expr = dsl.interval(missingRef, DSL.literal("day"));
    assertEquals(INTERVAL, expr.type());
    assertEquals(missingValue(), expr.valueOf(env));
  }
}
