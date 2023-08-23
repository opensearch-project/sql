/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class DateTimeTest extends ExpressionTestBase {

  @Test
  public void noTimeZoneNoField2() {
    FunctionExpression expr = DSL.datetime(DSL.literal("2008-05-15 22:00:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2008-05-15 22:00:00"), expr.valueOf());
  }

  @Test
  public void positiveTimeZoneNoField2() {
    FunctionExpression expr = DSL.datetime(DSL.literal("2008-05-15 22:00:00+01:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2008-05-15 22:00:00"), expr.valueOf());
  }

  @Test
  public void positiveField1WrittenField2() {
    FunctionExpression expr =
        DSL.datetime(DSL.literal("2008-05-15 22:00:00+01:00"), DSL.literal("America/Los_Angeles"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2008-05-15 14:00:00"), expr.valueOf());
  }

  // When no timezone argument is passed inside the datetime field, it assumes local time.
  @Test
  public void localDateTimeConversion() {
    // needs to work for all time zones because it defaults to local timezone.
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    String dt = "2008-05-15 22:00:00";
    String timeZone = "America/Los_Angeles";
    LocalDateTime timeConverted = LocalDateTime.parse(dt, formatter);
    ZonedDateTime timeZoneLocal =
        timeConverted
            .atZone(ZoneId.of(TimeZone.getDefault().getID()))
            .withZoneSameInstant(ZoneId.of(timeZone));
    FunctionExpression expr = DSL.datetime(DSL.literal(dt), DSL.literal(timeZone));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue(timeZoneLocal.toLocalDateTime()), expr.valueOf());
  }

  @Test
  public void negativeField1WrittenField2() {
    FunctionExpression expr =
        DSL.datetime(DSL.literal("2008-05-15 22:00:00-11:00"), DSL.literal("America/Los_Angeles"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2008-05-16 02:00:00"), expr.valueOf());
  }

  @Test
  public void negativeField1PositiveField2() {
    FunctionExpression expr =
        DSL.datetime(DSL.literal("2008-05-15 22:00:00-12:00"), DSL.literal("+15:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void twentyFourHourDifference() {
    FunctionExpression expr =
        DSL.datetime(DSL.literal("2008-05-15 22:00:00-14:00"), DSL.literal("+10:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void negativeToNull() {
    FunctionExpression expr =
        DSL.datetime(DSL.literal("2008-05-15 22:00:00-11:00"), DSL.literal(nullValue()));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }

  @Test
  public void invalidDate() {
    FunctionExpression expr = DSL.datetime(DSL.literal("2008-04-31 22:00:00-11:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(nullValue(), expr.valueOf());
  }
}
