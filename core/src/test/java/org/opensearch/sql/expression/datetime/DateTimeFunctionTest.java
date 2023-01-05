/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.missingValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.INTERVAL;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.util.List;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDatetimeValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;

@ExtendWith(MockitoExtension.class)
class DateTimeFunctionTest extends ExpressionTestBase {

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

  final List<DateFormatTester> dateFormatTesters = ImmutableList.of(
      new DateFormatTester("1998-01-31 13:14:15.012345",
          ImmutableList.of("%H","%I","%k","%l","%i","%p","%r","%S","%T"," %M",
              "%W","%D","%Y","%y","%a","%b","%j","%m","%d","%h","%s","%w","%f",
              "%q","%"),
          ImmutableList.of("13","01","13","1","14","PM","01:14:15 PM","15","13:14:15"," January",
              "Saturday","31st","1998","98","Sat","Jan","031","01","31","01","15","6","12345",
              "q","%")
      ),
      new DateFormatTester("1999-12-01",
          ImmutableList.of("%D"),
          ImmutableList.of("1st")
      ),
      new DateFormatTester("1999-12-02",
          ImmutableList.of("%D"),
          ImmutableList.of("2nd")
      ),
      new DateFormatTester("1999-12-03",
          ImmutableList.of("%D"),
          ImmutableList.of("3rd")
      ),
      new DateFormatTester("1999-12-04",
          ImmutableList.of("%D"),
          ImmutableList.of("4th")
      ),
      new DateFormatTester("1999-12-11",
          ImmutableList.of("%D"),
          ImmutableList.of("11th")
      ),
      new DateFormatTester("1999-12-12",
          ImmutableList.of("%D"),
          ImmutableList.of("12th")
      ),
      new DateFormatTester("1999-12-13",
          ImmutableList.of("%D"),
          ImmutableList.of("13th")
      ),
      new DateFormatTester("1999-12-31",
          ImmutableList.of("%x","%v","%X","%V","%u","%U"),
          ImmutableList.of("1999", "52", "1999", "52", "52", "52")
      ),
      new DateFormatTester("2000-01-01",
          ImmutableList.of("%x","%v","%X","%V","%u","%U"),
          ImmutableList.of("1999", "52", "1999", "52", "0", "0")
      ),
      new DateFormatTester("1998-12-31",
          ImmutableList.of("%x","%v","%X","%V","%u","%U"),
          ImmutableList.of("1998", "52", "1998", "52", "52", "52")
      ),
      new DateFormatTester("1999-01-01",
          ImmutableList.of("%x","%v","%X","%V","%u","%U"),
          ImmutableList.of("1998", "52", "1998", "52", "0", "0")
      ),
      new DateFormatTester("2020-01-04",
          ImmutableList.of("%x","%X"),
          ImmutableList.of("2020", "2019")
      ),
      new DateFormatTester("2008-12-31",
          ImmutableList.of("%v","%V","%u","%U"),
          ImmutableList.of("53","52","53","52")
      ),
      new DateFormatTester("1998-01-31 13:14:15.012345",
          ImmutableList.of("%Y-%m-%dT%TZ"),
          ImmutableList.of("1998-01-31T13:14:15Z")
      ),
      new DateFormatTester("1998-01-31 13:14:15.012345",
          ImmutableList.of("%Y-%m-%da %T a"),
          ImmutableList.of("1998-01-31PM 13:14:15 PM")
      ),
      new DateFormatTester("1998-01-31 13:14:15.012345",
          ImmutableList.of("%Y-%m-%db %T b"),
          ImmutableList.of("1998-01-31b 13:14:15 b"))
  );

  @AllArgsConstructor
  private class DateFormatTester {
    private final String date;
    private final List<String> formatterList;
    private final List<String> formattedList;
    private static final String DELIMITER = "|";

    String getFormatter() {
      return String.join(DELIMITER, formatterList);
    }

    String getFormatted() {
      return String.join(DELIMITER, formattedList);
    }

    FunctionExpression getDateFormatExpression() {
      return DSL.date_format(DSL.literal(date), DSL.literal(getFormatter()));
    }
  }

  @Test
  public void adddate() {
    FunctionExpression expr = DSL.adddate(DSL.date(DSL.literal("2020-08-26")), DSL.literal(7));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-09-02"), expr.valueOf(env));
    assertEquals("adddate(date(\"2020-08-26\"), 7)", expr.toString());

    expr = DSL.adddate(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-09-02 12:05:00"), expr.valueOf(env));
    assertEquals("adddate(timestamp(\"2020-08-26 12:05:00\"), 7)", expr.toString());

    expr = DSL.adddate(
        DSL.date(DSL.literal("2020-08-26")), DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 01:00:00"), expr.valueOf(env));
    assertEquals("adddate(date(\"2020-08-26\"), interval(1, \"hour\"))", expr.toString());

    expr = DSL.adddate(DSL.literal("2020-08-26"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDateValue("2020-09-02"), expr.valueOf(env));
    assertEquals("adddate(\"2020-08-26\", 7)", expr.toString());

    expr = DSL.adddate(DSL.literal("2020-08-26 12:05:00"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-09-02 12:05:00"), expr.valueOf(env));
    assertEquals("adddate(\"2020-08-26 12:05:00\", 7)", expr.toString());

    expr = DSL
        .adddate(DSL.literal("2020-08-26"), DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 01:00:00"), expr.valueOf(env));
    assertEquals("adddate(\"2020-08-26\", interval(1, \"hour\"))", expr.toString());

    expr = DSL
        .adddate(DSL.literal("2020-08-26"), DSL.interval(DSL.literal(1), DSL.literal("day")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDateValue("2020-08-27"), expr.valueOf(env));
    assertEquals("adddate(\"2020-08-26\", interval(1, \"day\"))", expr.toString());

    when(nullRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.adddate(nullRef, DSL.literal(1L))));
    assertEquals(nullValue(),
        eval(DSL.adddate(nullRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(missingRef.type()).thenReturn(DATE);
    assertEquals(missingValue(), eval(DSL.adddate(missingRef, DSL.literal(1L))));
    assertEquals(missingValue(),
        eval(DSL.adddate(missingRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(nullRef.type()).thenReturn(LONG);
    when(missingRef.type()).thenReturn(LONG);
    assertEquals(nullValue(), eval(DSL.adddate(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.adddate(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(INTERVAL);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(nullValue(), eval(DSL.adddate(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.adddate(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(missingValue(), eval(DSL.adddate(nullRef, missingRef)));
  }

  @Test
  public void date() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.date(nullRef)));
    assertEquals(missingValue(), eval(DSL.date(missingRef)));

    FunctionExpression expr = DSL.date(DSL.literal("2020-08-17"));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17"), eval(expr));
    assertEquals("date(\"2020-08-17\")", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17 12:12:00")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17 12:12:00"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17 12:12")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17 12:12"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());


  }

  @Test
  public void date_add() {
    FunctionExpression expr = DSL.date_add(DSL.date(DSL.literal("2020-08-26")), DSL.literal(7));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-09-02"), expr.valueOf(env));
    assertEquals("date_add(date(\"2020-08-26\"), 7)", expr.toString());

    expr = DSL.date_add(DSL.literal("2020-08-26 12:05:00"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-09-02 12:05:00"), expr.valueOf(env));
    assertEquals("date_add(\"2020-08-26 12:05:00\", 7)", expr.toString());

    expr = DSL.date_add(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-09-02 12:05:00"), expr.valueOf(env));
    assertEquals("date_add(timestamp(\"2020-08-26 12:05:00\"), 7)", expr.toString());

    expr = DSL.date_add(
        DSL.date(DSL.literal("2020-08-26")), DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 01:00:00"), expr.valueOf(env));
    assertEquals("date_add(date(\"2020-08-26\"), interval(1, \"hour\"))", expr.toString());

    expr = DSL
        .date_add(DSL.literal("2020-08-26"), DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 01:00:00"), expr.valueOf(env));
    assertEquals("date_add(\"2020-08-26\", interval(1, \"hour\"))", expr.toString());

    when(nullRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.date_add(nullRef, DSL.literal(1L))));
    assertEquals(nullValue(),
        eval(DSL.date_add(nullRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(missingRef.type()).thenReturn(DATE);
    assertEquals(missingValue(), eval(DSL.date_add(missingRef, DSL.literal(1L))));
    assertEquals(missingValue(),
        eval(DSL.date_add(missingRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(nullRef.type()).thenReturn(LONG);
    when(missingRef.type()).thenReturn(LONG);
    assertEquals(nullValue(), eval(DSL.date_add(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.date_add(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(INTERVAL);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(nullValue(), eval(DSL.date_add(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.date_add(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(missingValue(), eval(DSL.date_add(nullRef, missingRef)));
  }

  @Test
  public void date_sub() {
    FunctionExpression expr = DSL.date_sub(DSL.date(DSL.literal("2020-08-26")), DSL.literal(7));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-19"), expr.valueOf(env));
    assertEquals("date_sub(date(\"2020-08-26\"), 7)", expr.toString());

    expr = DSL.date_sub(DSL.literal("2020-08-26"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDateValue("2020-08-19"), expr.valueOf(env));
    assertEquals("date_sub(\"2020-08-26\", 7)", expr.toString());

    expr = DSL.date_sub(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-19 12:05:00"), expr.valueOf(env));
    assertEquals("date_sub(timestamp(\"2020-08-26 12:05:00\"), 7)", expr.toString());

    expr = DSL.date_sub(DSL.literal("2020-08-26 12:05:00"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-19 12:05:00"), expr.valueOf(env));
    assertEquals("date_sub(\"2020-08-26 12:05:00\", 7)", expr.toString());

    expr = DSL.date_sub(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")),
        DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 11:05:00"), expr.valueOf(env));
    assertEquals("date_sub(timestamp(\"2020-08-26 12:05:00\"), interval(1, \"hour\"))",
        expr.toString());

    expr = DSL.date_sub(DSL.literal("2020-08-26 12:05:00"),
        DSL.interval(DSL.literal(1), DSL.literal("year")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2019-08-26 12:05:00"), expr.valueOf(env));
    assertEquals("date_sub(\"2020-08-26 12:05:00\", interval(1, \"year\"))",
        expr.toString());

    when(nullRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.date_sub(nullRef, DSL.literal(1L))));
    assertEquals(nullValue(),
        eval(DSL.date_sub(nullRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(missingRef.type()).thenReturn(DATE);
    assertEquals(missingValue(), eval(DSL.date_sub(missingRef, DSL.literal(1L))));
    assertEquals(missingValue(),
        eval(DSL.date_sub(missingRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(nullRef.type()).thenReturn(LONG);
    when(missingRef.type()).thenReturn(LONG);
    assertEquals(nullValue(), eval(DSL.date_sub(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.date_sub(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(INTERVAL);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(nullValue(), eval(DSL.date_sub(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.date_sub(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(missingValue(), eval(DSL.date_sub(nullRef, missingRef)));
  }

  @Test
  public void day() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.day(nullRef)));
    assertEquals(missingValue(), eval(DSL.day(missingRef)));

    FunctionExpression expression = DSL.day(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("day(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(7), eval(expression));

    expression = DSL.day(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("day(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(7), eval(expression));
  }

  @Test
  public void dayName() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.dayname(nullRef)));
    assertEquals(missingValue(), eval(DSL.dayname(missingRef)));

    FunctionExpression expression = DSL.dayname(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(STRING, expression.type());
    assertEquals("dayname(DATE '2020-08-07')", expression.toString());
    assertEquals(stringValue("Friday"), eval(expression));

    expression = DSL.dayname(DSL.literal("2020-08-07"));
    assertEquals(STRING, expression.type());
    assertEquals("dayname(\"2020-08-07\")", expression.toString());
    assertEquals(stringValue("Friday"), eval(expression));
  }

  @Test
  public void dayOfMonth() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.dayofmonth(nullRef)));
    assertEquals(missingValue(), eval(DSL.dayofmonth(missingRef)));

    FunctionExpression expression = DSL.dayofmonth(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofmonth(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(7), eval(expression));

    expression = DSL.dayofmonth(DSL.literal("2020-07-08"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofmonth(\"2020-07-08\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  private void dayOfWeekQuery(
      FunctionExpression dateExpression,
      int dayOfWeek,
      String testExpr) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(dayOfWeek), eval(dateExpression));
    assertEquals(testExpr, dateExpression.toString());
  }

  @Test
  public void dayOfWeek() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    FunctionExpression expression1 = DSL.dayofweek(
        functionProperties,
        DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 = DSL.dayofweek(
        functionProperties,
        DSL.literal(new ExprDateValue("2020-08-09")));
    FunctionExpression expression3 = DSL.dayofweek(
        functionProperties,
        DSL.literal("2020-08-09"));
    FunctionExpression expression4 = DSL.dayofweek(
        functionProperties,
        DSL.literal("2020-08-09 01:02:03"));

    assertAll(
        () -> dayOfWeekQuery(expression1, 6, "dayofweek(DATE '2020-08-07')"),

        () -> dayOfWeekQuery(expression2, 1, "dayofweek(DATE '2020-08-09')"),

        () -> dayOfWeekQuery(expression3, 1, "dayofweek(\"2020-08-09\")"),

        () -> dayOfWeekQuery(expression4, 1, "dayofweek(\"2020-08-09 01:02:03\")")
    );
  }

  private void dayOfWeekWithUnderscoresQuery(
      FunctionExpression dateExpression,
      int dayOfWeek,
      String testExpr) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(dayOfWeek), eval(dateExpression));
    assertEquals(testExpr, dateExpression.toString());
  }

  @Test
  public void dayOfWeekWithUnderscores() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    FunctionExpression expression1 = DSL.day_of_week(
        functionProperties,
        DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 = DSL.day_of_week(
        functionProperties,
        DSL.literal(new ExprDateValue("2020-08-09")));
    FunctionExpression expression3 = DSL.day_of_week(
        functionProperties,
        DSL.literal("2020-08-09"));
    FunctionExpression expression4 = DSL.day_of_week(
        functionProperties,
        DSL.literal("2020-08-09 01:02:03"));

    assertAll(
        () -> dayOfWeekWithUnderscoresQuery(expression1, 6, "day_of_week(DATE '2020-08-07')"),

        () -> dayOfWeekWithUnderscoresQuery(expression2, 1, "day_of_week(DATE '2020-08-09')"),

        () -> dayOfWeekWithUnderscoresQuery(expression3, 1, "day_of_week(\"2020-08-09\")"),

        () -> dayOfWeekWithUnderscoresQuery(
            expression4, 1, "day_of_week(\"2020-08-09 01:02:03\")")
    );
  }

  @Test
  public void testDayOfWeekWithTimeType() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());
    FunctionExpression expression = DSL.day_of_week(
        functionProperties, DSL.literal(new ExprTimeValue("12:23:34")));

    assertAll(
        () -> assertEquals(INTEGER, eval(expression).type()),
        () -> assertEquals((
            LocalDate.now(
                functionProperties.getQueryStartClock()).getDayOfWeek().getValue() % 7) + 1,
            eval(expression).integerValue()),
        () -> assertEquals("day_of_week(TIME '12:23:34')", expression.toString())
    );
  }

  private void testInvalidDayOfWeek(String date) {
    FunctionExpression expression = DSL.day_of_week(
        functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void dayOfWeekWithUnderscoresLeapYear() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    assertAll(
        //Feb. 29 of a leap year
        () -> dayOfWeekWithUnderscoresQuery(DSL.day_of_week(
            functionProperties,
            DSL.literal("2020-02-29")), 7, "day_of_week(\"2020-02-29\")"),
        //day after Feb. 29 of a leap year
        () -> dayOfWeekWithUnderscoresQuery(DSL.day_of_week(
            functionProperties,
            DSL.literal("2020-03-01")), 1, "day_of_week(\"2020-03-01\")"),
        //Feb. 28 of a non-leap year
        () -> dayOfWeekWithUnderscoresQuery(DSL.day_of_week(
            functionProperties,
            DSL.literal("2021-02-28")), 1, "day_of_week(\"2021-02-28\")"),
        //Feb. 29 of a non-leap year
        () -> assertThrows(
            SemanticCheckException.class, () ->  testInvalidDayOfWeek("2021-02-29"))
    );
  }

  @Test
  public void dayOfWeekWithUnderscoresInvalidArgument() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.day_of_week(functionProperties, nullRef)));
    assertEquals(missingValue(), eval(DSL.day_of_week(functionProperties, missingRef)));

    assertAll(
        //40th day of the month
        () -> assertThrows(SemanticCheckException.class,
            () ->  testInvalidDayOfWeek("2021-02-40")),

        //13th month of the year
        () -> assertThrows(SemanticCheckException.class,
            () ->  testInvalidDayOfWeek("2021-13-29")),

        //incorrect format
        () -> assertThrows(SemanticCheckException.class,
            () ->  testInvalidDayOfWeek("asdfasdf"))
    );
  }

  @Test
  public void dayOfYear() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.dayofyear(nullRef)));
    assertEquals(missingValue(), eval(DSL.dayofyear(missingRef)));

    FunctionExpression expression = DSL.dayofyear(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(220), eval(expression));

    expression = DSL.dayofyear(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(220), eval(expression));

    expression = DSL.dayofyear(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(220), eval(expression));
  }

  private void testDayOfYearWithUnderscores(String date, int dayOfYear) {
    FunctionExpression expression = DSL.day_of_year(DSL.literal(new ExprDateValue(date)));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(dayOfYear), eval(expression));
  }

  @Test
  public void dayOfYearWithUnderscoresDifferentArgumentFormats() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    FunctionExpression expression1 = DSL.day_of_year(DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 = DSL.day_of_year(DSL.literal("2020-08-07"));
    FunctionExpression expression3 = DSL.day_of_year(DSL.literal("2020-08-07 01:02:03"));

    assertAll(
        () -> testDayOfYearWithUnderscores("2020-08-07", 220),
        () -> assertEquals("day_of_year(DATE '2020-08-07')", expression1.toString()),

        () -> testDayOfYearWithUnderscores("2020-08-07", 220),
        () ->     assertEquals("day_of_year(\"2020-08-07\")", expression2.toString()),

        () -> testDayOfYearWithUnderscores("2020-08-07 01:02:03", 220),
        () ->     assertEquals("day_of_year(\"2020-08-07 01:02:03\")", expression3.toString())
    );
  }

  @Test
  public void dayOfYearWithUnderscoresCornerCaseDates() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    assertAll(
        //31st of December during non leap year (should be 365)
        () -> testDayOfYearWithUnderscores("2019-12-31", 365),
        //Year 1200
        () -> testDayOfYearWithUnderscores("1200-02-28", 59),
        //Year 4000
        () -> testDayOfYearWithUnderscores("4000-02-28", 59)
    );
  }

  @Test
  public void dayOfYearWithUnderscoresLeapYear() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    assertAll(
        //28th of Feb
        () -> testDayOfYearWithUnderscores("2020-02-28", 59),

        //29th of Feb during leap year
        () -> testDayOfYearWithUnderscores("2020-02-29 23:59:59", 60),
        () -> testDayOfYearWithUnderscores("2020-02-29", 60),

        //1st of March during leap year
        () -> testDayOfYearWithUnderscores("2020-03-01 00:00:00", 61),
        () -> testDayOfYearWithUnderscores("2020-03-01", 61),

        //1st of March during non leap year
        () -> testDayOfYearWithUnderscores("2019-03-01", 60),

        //31st of December during  leap year (should be 366)
        () -> testDayOfYearWithUnderscores("2020-12-31", 366)
    );
  }

  private void testInvalidDayOfYear(String date) {
    FunctionExpression expression = DSL.day_of_year(DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void invalidDayOfYearArgument() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.day_of_year(nullRef)));
    assertEquals(missingValue(), eval(DSL.day_of_year(missingRef)));

    //29th of Feb non-leapyear
    assertThrows(SemanticCheckException.class, () ->  testInvalidDayOfYear("2019-02-29"));
    //13th month
    assertThrows(SemanticCheckException.class, () ->  testInvalidDayOfYear("2019-13-15"));
    //incorrect format for type
    assertThrows(SemanticCheckException.class, () ->  testInvalidDayOfYear("asdfasdfasdf"));
  }
  
  @Test
  public void from_days() {
    when(nullRef.type()).thenReturn(LONG);
    when(missingRef.type()).thenReturn(LONG);
    assertEquals(nullValue(), eval(DSL.from_days(nullRef)));
    assertEquals(missingValue(), eval(DSL.from_days(missingRef)));

    FunctionExpression expression = DSL.from_days(DSL.literal(new ExprLongValue(730669)));
    assertEquals(DATE, expression.type());
    assertEquals("from_days(730669)", expression.toString());
    assertEquals(new ExprDateValue("2000-07-03"), expression.valueOf(env));
  }

  @Test
  public void hour() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.hour(nullRef)));
    assertEquals(missingValue(), eval(DSL.hour(missingRef)));

    FunctionExpression expression = DSL.hour(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), eval(expression));
    assertEquals("hour(TIME '01:02:03')", expression.toString());

    expression = DSL.hour(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), eval(expression));
    assertEquals("hour(\"01:02:03\")", expression.toString());

    expression = DSL.hour(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), expression.valueOf(env));
    assertEquals("hour(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.hour(DSL.literal(new ExprDatetimeValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), expression.valueOf(env));
    assertEquals("hour(DATETIME '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.hour(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), expression.valueOf(env));
    assertEquals("hour(\"2020-08-17 01:02:03\")", expression.toString());
  }

  @Test
  public void microsecond() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.microsecond(nullRef)));
    assertEquals(missingValue(), eval(DSL.microsecond(missingRef)));

    FunctionExpression expression = DSL
        .microsecond(DSL.literal(new ExprTimeValue("01:02:03.123456")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), eval(expression));
    assertEquals("microsecond(TIME '01:02:03.123456')", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprTimeValue("01:02:03.00")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(0), eval(expression));
    assertEquals("microsecond(TIME '01:02:03')", expression.toString());

    expression = DSL.microsecond(DSL.literal("01:02:03.12"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(120000), eval(expression));
    assertEquals("microsecond(\"01:02:03.12\")", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprDatetimeValue("2020-08-17 01:02:03.000010")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(10), expression.valueOf(env));
    assertEquals("microsecond(DATETIME '2020-08-17 01:02:03.00001')", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprDatetimeValue("2020-08-17 01:02:03.123456")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), expression.valueOf(env));
    assertEquals("microsecond(DATETIME '2020-08-17 01:02:03.123456')", expression.toString());

    expression = DSL.microsecond(DSL.literal("2020-08-17 01:02:03.123456"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), expression.valueOf(env));
    assertEquals("microsecond(\"2020-08-17 01:02:03.123456\")", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03.000010")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(10), expression.valueOf(env));
    assertEquals("microsecond(TIMESTAMP '2020-08-17 01:02:03.00001')", expression.toString());
  }

  @Test
  public void minute() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.minute(nullRef)));
    assertEquals(missingValue(), eval(DSL.minute(missingRef)));

    FunctionExpression expression = DSL.minute(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), eval(expression));
    assertEquals("minute(TIME '01:02:03')", expression.toString());

    expression = DSL.minute(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), eval(expression));
    assertEquals("minute(\"01:02:03\")", expression.toString());

    expression = DSL.minute(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), expression.valueOf(env));
    assertEquals("minute(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.minute(DSL.literal(new ExprDatetimeValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), expression.valueOf(env));
    assertEquals("minute(DATETIME '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.minute(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), expression.valueOf(env));
    assertEquals("minute(\"2020-08-17 01:02:03\")", expression.toString());
  }

  @Test
  public void month() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.month(nullRef)));
    assertEquals(missingValue(), eval(DSL.month(missingRef)));

    FunctionExpression expression = DSL.month(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  private void testInvalidDates(String date) throws SemanticCheckException {
    FunctionExpression expression = DSL.month_of_year(DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test void monthOfYearInvalidDates() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.month_of_year(nullRef)));
    assertEquals(missingValue(), eval(DSL.month_of_year(missingRef)));

    assertThrows(SemanticCheckException.class, () ->  testInvalidDates("2019-01-50"));
    assertThrows(SemanticCheckException.class, () ->  testInvalidDates("2019-02-29"));
    assertThrows(SemanticCheckException.class, () ->  testInvalidDates("2019-02-31"));
    assertThrows(SemanticCheckException.class, () ->  testInvalidDates("2019-13-05"));
  }

  @Test
  public void monthOfYearAlternateArgumentSyntaxes() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    FunctionExpression expression = DSL.month_of_year(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month_of_year(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month_of_year(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  @Test
  public void monthName() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.monthname(nullRef)));
    assertEquals(missingValue(), eval(DSL.monthname(missingRef)));

    FunctionExpression expression = DSL.monthname(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(DATE '2020-08-07')", expression.toString());
    assertEquals(stringValue("August"), eval(expression));

    expression = DSL.monthname(DSL.literal("2020-08-07"));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(\"2020-08-07\")", expression.toString());
    assertEquals(stringValue("August"), eval(expression));

    expression = DSL.monthname(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(stringValue("August"), eval(expression));
  }

  @Test
  public void quarter() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.quarter(nullRef)));
    assertEquals(missingValue(), eval(DSL.quarter(missingRef)));

    FunctionExpression expression = DSL.quarter(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(3), eval(expression));

    expression = DSL.quarter(DSL.literal("2020-12-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(\"2020-12-07\")", expression.toString());
    assertEquals(integerValue(4), eval(expression));

    expression = DSL.quarter(DSL.literal("2020-12-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(\"2020-12-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(4), eval(expression));
  }

  @Test
  public void second() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.second(nullRef)));
    assertEquals(missingValue(), eval(DSL.second(missingRef)));

    FunctionExpression expression = DSL.second(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(TIME '01:02:03')", expression.toString());

    expression = DSL.second(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(\"01:02:03\")", expression.toString());

    expression = DSL.second(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(\"2020-08-17 01:02:03\")", expression.toString());

    expression = DSL.second(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), expression.valueOf(env));
    assertEquals("second(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.second(DSL.literal(new ExprDatetimeValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), expression.valueOf(env));
    assertEquals("second(DATETIME '2020-08-17 01:02:03')", expression.toString());
  }

  @Test
  public void subdate() {
    FunctionExpression expr = DSL.subdate(DSL.date(DSL.literal("2020-08-26")), DSL.literal(7));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-19"), expr.valueOf(env));
    assertEquals("subdate(date(\"2020-08-26\"), 7)", expr.toString());

    expr = DSL.subdate(DSL.literal("2020-08-26"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDateValue("2020-08-19"), expr.valueOf(env));
    assertEquals("subdate(\"2020-08-26\", 7)", expr.toString());

    expr = DSL.subdate(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-19 12:05:00"), expr.valueOf(env));
    assertEquals("subdate(timestamp(\"2020-08-26 12:05:00\"), 7)", expr.toString());

    expr = DSL.subdate(DSL.literal("2020-08-26 12:05:00"), DSL.literal(7));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-19 12:05:00"), expr.valueOf(env));
    assertEquals("subdate(\"2020-08-26 12:05:00\", 7)", expr.toString());

    expr = DSL.subdate(DSL.timestamp(DSL.literal("2020-08-26 12:05:00")),
        DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 11:05:00"), expr.valueOf(env));
    assertEquals("subdate(timestamp(\"2020-08-26 12:05:00\"), interval(1, \"hour\"))",
        expr.toString());

    expr = DSL.subdate(DSL.literal("2020-08-26 12:05:00"),
        DSL.interval(DSL.literal(1), DSL.literal("hour")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDatetimeValue("2020-08-26 11:05:00"), expr.valueOf(env));
    assertEquals("subdate(\"2020-08-26 12:05:00\", interval(1, \"hour\"))",
        expr.toString());

    expr = DSL.subdate(DSL.literal("2020-08-26"),
        DSL.interval(DSL.literal(1), DSL.literal("day")));
    assertEquals(DATETIME, expr.type());
    assertEquals(new ExprDateValue("2020-08-25"), expr.valueOf(env));
    assertEquals("subdate(\"2020-08-26\", interval(1, \"day\"))",
        expr.toString());

    when(nullRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.subdate(nullRef, DSL.literal(1L))));
    assertEquals(nullValue(),
        eval(DSL.subdate(nullRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(missingRef.type()).thenReturn(DATE);
    assertEquals(missingValue(), eval(DSL.subdate(missingRef, DSL.literal(1L))));
    assertEquals(missingValue(),
        eval(DSL.subdate(missingRef, DSL.interval(DSL.literal(1), DSL.literal("month")))));

    when(nullRef.type()).thenReturn(LONG);
    when(missingRef.type()).thenReturn(LONG);
    assertEquals(nullValue(), eval(DSL.subdate(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.subdate(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(INTERVAL);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(nullValue(), eval(DSL.subdate(DSL.date(DSL.literal("2020-08-26")), nullRef)));
    assertEquals(missingValue(),
        eval(DSL.subdate(DSL.date(DSL.literal("2020-08-26")), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTERVAL);
    assertEquals(missingValue(), eval(DSL.subdate(nullRef, missingRef)));
  }

  @Test
  public void time_to_sec() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.time_to_sec(nullRef)));
    assertEquals(missingValue(), eval(DSL.time_to_sec(missingRef)));

    FunctionExpression expression = DSL.time_to_sec(DSL.literal(new ExprTimeValue("22:23:00")));
    assertEquals(LONG, expression.type());
    assertEquals("time_to_sec(TIME '22:23:00')", expression.toString());
    assertEquals(longValue(80580L), eval(expression));

    expression = DSL.time_to_sec(DSL.literal("22:23:00"));
    assertEquals(LONG, expression.type());
    assertEquals("time_to_sec(\"22:23:00\")", expression.toString());
    assertEquals(longValue(80580L), eval(expression));
  }

  @Test
  public void time() {
    when(nullRef.type()).thenReturn(TIME);
    when(missingRef.type()).thenReturn(TIME);
    assertEquals(nullValue(), eval(DSL.time(nullRef)));
    assertEquals(missingValue(), eval(DSL.time(missingRef)));

    FunctionExpression expr = DSL.time(DSL.literal("01:01:01"));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01"), eval(expr));
    assertEquals("time(\"01:01:01\")", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01"), eval(expr));
    assertEquals("time(TIME '01:01:01')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01"), eval(expr));
    assertEquals("time(TIME '01:01:00')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("2019-04-19 01:01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("2019-04-19 01:01:01"), eval(expr));
    assertEquals("time(TIME '01:01:01')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("2019-04-19 01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("2019-04-19 01:01"), eval(expr));
    assertEquals("time(TIME '01:01:00')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01:01.0123")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01.0123"), eval(expr));
    assertEquals("time(TIME '01:01:01.0123')", expr.toString());

    expr = DSL.time(DSL.date(DSL.literal("2020-01-02")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("00:00:00"), expr.valueOf());
  }

  @Test
  public void timestamp() {
    when(nullRef.type()).thenReturn(TIMESTAMP);
    when(missingRef.type()).thenReturn(TIMESTAMP);
    assertEquals(nullValue(), eval(DSL.timestamp(nullRef)));
    assertEquals(missingValue(), eval(DSL.timestamp(missingRef)));

    FunctionExpression expr = DSL.timestamp(DSL.literal("2020-08-17 01:01:01"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2020-08-17 01:01:01"), expr.valueOf(env));
    assertEquals("timestamp(\"2020-08-17 01:01:01\")", expr.toString());

    expr = DSL.timestamp(DSL.literal(new ExprTimestampValue("2020-08-17 01:01:01")));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2020-08-17 01:01:01"), expr.valueOf(env));
    assertEquals("timestamp(TIMESTAMP '2020-08-17 01:01:01')", expr.toString());
  }

  private void testWeek(String date, int mode, int expectedResult) {
    FunctionExpression expression = DSL
        .week(DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("week(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private void testNullMissingWeek(ExprCoreType date) {
    when(nullRef.type()).thenReturn(date);
    when(missingRef.type()).thenReturn(date);
    assertEquals(nullValue(), eval(DSL.week(nullRef)));
    assertEquals(missingValue(), eval(DSL.week(missingRef)));
  }

  @Test
  public void week() {
    testNullMissingWeek(DATE);
    testNullMissingWeek(DATETIME);
    testNullMissingWeek(TIMESTAMP);
    testNullMissingWeek(STRING);

    when(nullRef.type()).thenReturn(INTEGER);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(DSL.week(DSL.literal("2019-01-05"), nullRef)));
    assertEquals(missingValue(), eval(DSL.week(DSL.literal("2019-01-05"), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(DSL.week(nullRef, missingRef)));

    FunctionExpression expression = DSL
        .week(DSL.literal(new ExprTimestampValue("2019-01-05 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals("week(TIMESTAMP '2019-01-05 01:02:03')", expression.toString());
    assertEquals(integerValue(0), eval(expression));

    expression = DSL.week(DSL.literal("2019-01-05"));
    assertEquals(INTEGER, expression.type());
    assertEquals("week(\"2019-01-05\")", expression.toString());
    assertEquals(integerValue(0), eval(expression));

    expression = DSL.week(DSL.literal("2019-01-05 00:01:00"));
    assertEquals(INTEGER, expression.type());
    assertEquals("week(\"2019-01-05 00:01:00\")", expression.toString());
    assertEquals(integerValue(0), eval(expression));

    testWeek("2019-01-05", 0, 0);
    testWeek("2019-01-05", 1, 1);
    testWeek("2019-01-05", 2, 52);
    testWeek("2019-01-05", 3, 1);
    testWeek("2019-01-05", 4, 1);
    testWeek("2019-01-05", 5, 0);
    testWeek("2019-01-05", 6, 1);
    testWeek("2019-01-05", 7, 53);

    testWeek("2019-01-06", 0, 1);
    testWeek("2019-01-06", 1, 1);
    testWeek("2019-01-06", 2, 1);
    testWeek("2019-01-06", 3, 1);
    testWeek("2019-01-06", 4, 2);
    testWeek("2019-01-06", 5, 0);
    testWeek("2019-01-06", 6, 2);
    testWeek("2019-01-06", 7, 53);

    testWeek("2019-01-07", 0, 1);
    testWeek("2019-01-07", 1, 2);
    testWeek("2019-01-07", 2, 1);
    testWeek("2019-01-07", 3, 2);
    testWeek("2019-01-07", 4, 2);
    testWeek("2019-01-07", 5, 1);
    testWeek("2019-01-07", 6, 2);
    testWeek("2019-01-07", 7, 1);

    testWeek("2000-01-01", 0, 0);
    testWeek("2000-01-01", 2, 52);
    testWeek("1999-12-31", 0, 52);
  }

  @Test
  public void modeInUnsupportedFormat() {
    testNullMissingWeek(DATE);

    FunctionExpression expression1 = DSL
        .week(DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(8));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> eval(expression1));
    assertEquals("mode:8 is invalid, please use mode value between 0-7",
        exception.getMessage());

    FunctionExpression expression2 = DSL
        .week(DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(-1));
    exception = assertThrows(SemanticCheckException.class, () -> eval(expression2));
    assertEquals("mode:-1 is invalid, please use mode value between 0-7",
        exception.getMessage());
  }

  private void testWeekOfYear(String date, int mode, int expectedResult) {
    FunctionExpression expression = DSL
        .week_of_year(DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("week_of_year(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private void testNullMissingWeekOfYear(ExprCoreType date) {
    when(nullRef.type()).thenReturn(date);
    when(missingRef.type()).thenReturn(date);
    assertEquals(nullValue(), eval(DSL.week_of_year(nullRef)));
    assertEquals(missingValue(), eval(DSL.week_of_year(missingRef)));
  }

  @Test
  public void testInvalidWeekOfYear() {
    testNullMissingWeekOfYear(DATE);
    testNullMissingWeekOfYear(DATETIME);
    testNullMissingWeekOfYear(TIMESTAMP);
    testNullMissingWeekOfYear(STRING);

    when(nullRef.type()).thenReturn(INTEGER);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(nullValue(), eval(DSL.week_of_year(DSL.literal("2019-01-05"), nullRef)));
    assertEquals(missingValue(), eval(DSL.week_of_year(DSL.literal("2019-01-05"), missingRef)));

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(INTEGER);
    assertEquals(missingValue(), eval(DSL.week_of_year(nullRef, missingRef)));

    //test invalid month
    assertThrows(SemanticCheckException.class, () -> testWeekOfYear("2019-13-05 01:02:03", 0, 0));
    //test invalid day
    assertThrows(SemanticCheckException.class, () -> testWeekOfYear("2019-01-50 01:02:03", 0, 0));
    //test invalid leap year
    assertThrows(SemanticCheckException.class, () -> testWeekOfYear("2019-02-29 01:02:03", 0, 0));
  }

  @Test
  public void testWeekOfYearAlternateArgumentFormats() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    FunctionExpression expression = DSL
        .week_of_year(DSL.literal(new ExprTimestampValue("2019-01-05 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals("week_of_year(TIMESTAMP '2019-01-05 01:02:03')", expression.toString());
    assertEquals(integerValue(0), eval(expression));

    expression = DSL.week_of_year(DSL.literal("2019-01-05"));
    assertEquals(INTEGER, expression.type());
    assertEquals("week_of_year(\"2019-01-05\")", expression.toString());
    assertEquals(integerValue(0), eval(expression));

    expression = DSL.week_of_year(DSL.literal("2019-01-05 00:01:00"));
    assertEquals(INTEGER, expression.type());
    assertEquals("week_of_year(\"2019-01-05 00:01:00\")", expression.toString());
    assertEquals(integerValue(0), eval(expression));
  }

  @Test
  public void testWeekOfYearDifferentModes() {
    lenient().when(nullRef.valueOf(env)).thenReturn(nullValue());
    lenient().when(missingRef.valueOf(env)).thenReturn(missingValue());

    //Test the behavior of different modes passed into the 'week_of_year' function
    testWeekOfYear("2019-01-05", 0, 0);
    testWeekOfYear("2019-01-05", 1, 1);
    testWeekOfYear("2019-01-05", 2, 52);
    testWeekOfYear("2019-01-05", 3, 1);
    testWeekOfYear("2019-01-05", 4, 1);
    testWeekOfYear("2019-01-05", 5, 0);
    testWeekOfYear("2019-01-05", 6, 1);
    testWeekOfYear("2019-01-05", 7, 53);

    testWeekOfYear("2019-01-06", 0, 1);
    testWeekOfYear("2019-01-06", 1, 1);
    testWeekOfYear("2019-01-06", 2, 1);
    testWeekOfYear("2019-01-06", 3, 1);
    testWeekOfYear("2019-01-06", 4, 2);
    testWeekOfYear("2019-01-06", 5, 0);
    testWeekOfYear("2019-01-06", 6, 2);
    testWeekOfYear("2019-01-06", 7, 53);

    testWeekOfYear("2019-01-07", 0, 1);
    testWeekOfYear("2019-01-07", 1, 2);
    testWeekOfYear("2019-01-07", 2, 1);
    testWeekOfYear("2019-01-07", 3, 2);
    testWeekOfYear("2019-01-07", 4, 2);
    testWeekOfYear("2019-01-07", 5, 1);
    testWeekOfYear("2019-01-07", 6, 2);
    testWeekOfYear("2019-01-07", 7, 1);

    testWeekOfYear("2000-01-01", 0, 0);
    testWeekOfYear("2000-01-01", 2, 52);
    testWeekOfYear("1999-12-31", 0, 52);

  }

  @Test
  public void weekOfYearModeInUnsupportedFormat() {
    testNullMissingWeekOfYear(DATE);

    FunctionExpression expression1 = DSL
        .week_of_year(DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(8));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> eval(expression1));
    assertEquals("mode:8 is invalid, please use mode value between 0-7",
        exception.getMessage());

    FunctionExpression expression2 = DSL
        .week_of_year(DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(-1));
    exception = assertThrows(SemanticCheckException.class, () -> eval(expression2));
    assertEquals("mode:-1 is invalid, please use mode value between 0-7",
        exception.getMessage());
  }

  @Test
  public void to_days() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.to_days(nullRef)));
    assertEquals(missingValue(), eval(DSL.to_days(missingRef)));

    FunctionExpression expression = DSL.to_days(DSL.literal(new ExprDateValue("2008-10-07")));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(DATE '2008-10-07')", expression.toString());
    assertEquals(longValue(733687L), eval(expression));

    expression = DSL.to_days(DSL.literal("1969-12-31"));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(\"1969-12-31\")", expression.toString());
    assertEquals(longValue(719527L), eval(expression));

    expression = DSL.to_days(DSL.literal("1969-12-31 01:01:01"));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(\"1969-12-31 01:01:01\")", expression.toString());
    assertEquals(longValue(719527L), eval(expression));
  }

  @Test
  public void year() {
    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.year(nullRef)));
    assertEquals(missingValue(), eval(DSL.year(missingRef)));

    FunctionExpression expression = DSL.year(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(2020), eval(expression));

    expression = DSL.year(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(2020), eval(expression));

    expression = DSL.year(DSL.literal("2020-08-07 01:01:01"));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(\"2020-08-07 01:01:01\")", expression.toString());
    assertEquals(integerValue(2020), eval(expression));
  }

  @Test
  public void date_format() {
    dateFormatTesters.forEach(this::testDateFormat);
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat = "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M "
        + "%m %p %r %S %s %T %% %P";
    String timestampFormatted = "Sat Jan 01 31st 31 31 12345 13 01 01 14 031 13 1 "
        + "January 01 PM 01:14:15 PM 15 15 13:14:15 % P";

    FunctionExpression expr = DSL.date_format(DSL.literal(timestamp), DSL.literal(timestampFormat));
    assertEquals(STRING, expr.type());
    assertEquals(timestampFormatted, eval(expr).stringValue());

    when(nullRef.type()).thenReturn(DATE);
    when(missingRef.type()).thenReturn(DATE);
    assertEquals(nullValue(), eval(DSL.date_format(nullRef, DSL.literal(""))));
    assertEquals(missingValue(), eval(DSL.date_format(missingRef, DSL.literal(""))));

    when(nullRef.type()).thenReturn(DATETIME);
    when(missingRef.type()).thenReturn(DATETIME);
    assertEquals(nullValue(), eval(DSL.date_format(nullRef, DSL.literal(""))));
    assertEquals(missingValue(), eval(DSL.date_format(missingRef, DSL.literal(""))));

    when(nullRef.type()).thenReturn(TIMESTAMP);
    when(missingRef.type()).thenReturn(TIMESTAMP);
    assertEquals(nullValue(), eval(DSL.date_format(nullRef, DSL.literal(""))));
    assertEquals(missingValue(), eval(DSL.date_format(missingRef, DSL.literal(""))));

    when(nullRef.type()).thenReturn(STRING);
    when(missingRef.type()).thenReturn(STRING);
    assertEquals(nullValue(), eval(DSL.date_format(nullRef, DSL.literal(""))));
    assertEquals(missingValue(), eval(DSL.date_format(missingRef, DSL.literal(""))));
    assertEquals(nullValue(), eval(DSL.date_format(DSL.literal(""), nullRef)));
    assertEquals(missingValue(), eval(DSL.date_format(DSL.literal(""), missingRef)));
  }

  void testDateFormat(DateFormatTester dft) {
    FunctionExpression expr = dft.getDateFormatExpression();
    assertEquals(STRING, expr.type());
    assertEquals(dft.getFormatted(), eval(expr).stringValue());
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf(env);
  }
}
