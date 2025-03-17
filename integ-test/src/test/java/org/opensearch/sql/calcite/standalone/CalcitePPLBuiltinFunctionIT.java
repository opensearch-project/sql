/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_STATE_COUNTRY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WILDCARD;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);
    Request request3 = new Request("PUT", "/test_name_null/_doc/1?refresh=true");
    request3.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request3);
    Request request4 = new Request("PUT", "/test_name_null/_doc/2?refresh=true");
    request4.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request4);
    Request request5 = new Request("PUT", "/test_name_null/_doc/3?refresh=true");
    request5.setJsonEntity("{\"name\": null, \"age\": 30}");
    client().performRequest(request5);

    Request request6 = new Request("PUT", "/people/_doc/2?refresh=true");
    request6.setJsonEntity("{\"name\": \"DummyEntityForMathVerification\", \"age\": 24}");
    client().performRequest(request6);

    loadIndex(Index.BANK);
  }

  @Test
  public void testUnixTimestamp() {
    String query =
        "source=people | eval `UNIX_TIMESTAMP(double)` = UNIX_TIMESTAMP(20771122143845),"
            + " `UNIX_TIMESTAMP(timestamp)` = UNIX_TIMESTAMP(TIMESTAMP('1996-11-15 17:05:42')) |"
            + " fields `UNIX_TIMESTAMP(double)`, `UNIX_TIMESTAMP(timestamp)`";
    testSimplePPL(query, List.of(3404817525.0, 848077542.0));
  }

  @Test
  public void testDate() {
    String query =
        "source=people | eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields"
            + " `DATE('2020-08-26')`";
    testSimplePPL(query, List.of("2020-08-26"));
    testSimplePPL(
        "source=people | eval `DATE('2020-08-26')` = DATE('2020-08-26') | fields"
            + " `DATE('2020-08-26')`",
        List.of("2020-08-26"));
    testSimplePPL(
        "source=people | eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26"
            + " 13:49:00')) | fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`",
        List.of("2020-08-26"));
    testSimplePPL(
        "source=people | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') | fields"
            + " `DATE('2020-08-26 13:49')`",
        List.of("2020-08-26"));
  }

  @Test
  public void testDate2() {
    String query =
        "source=people |eval `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26"
            + " 13:49:00')) | fields `DATE(TIMESTAMP('2020-08-26 13:49:00'))`";
    testSimplePPL(query, List.of("2020-08-26"));
  }

  @Test
  public void testUTCTIMESTAMP() {
    Instant utcTimestamp = Instant.now();
    String query =
        "source=people | eval `UTC_TIMESTAMP()` = UTC_TIMESTAMP() | fields `UTC_TIMESTAMP()`";
    DateTimeFormatter formatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"));
    String formattedString = formatter.format(utcTimestamp);
    testSimplePPL(query, List.of(formattedString));
  }

  @Test
  public void testUTCTIME() {
    Instant utcTimestamp = Instant.now();
    LocalTime time = utcTimestamp.atZone(ZoneId.of("UTC")).toLocalTime();

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    String formattedTime = time.format(formatter);
    String query = "source=people | eval `UTC_TIME()` = UTC_TIME() | fields `UTC_TIME()`";
    testSimplePPL(query, List.of(formattedTime.toString()));
  }

  @Test
  public void testUTCDATE() {
    Instant utcTimestamp = Instant.now();
    LocalDate localDate = utcTimestamp.atZone(ZoneId.of("UTC")).toLocalDate();
    String query = "source=people | eval `UTC_DATE()` = UTC_DATE() | fields `UTC_DATE()`";
    testSimplePPL(query, List.of(localDate.toString()));
  }

  @Test
  public void testTimestampForOneInput() {
    String query =
        "source=people | eval `TIMESTAMP('2020-08-26 13:49:00', '2020-08-26 13:49:02')` ="
            + " TIMESTAMP('2020-08-26 13:49:00', '2020-08-26 13:49:02')| fields"
            + " `TIMESTAMP('2020-08-26 13:49:00', '2020-08-26 13:49:02')`";
    testSimplePPL(query, List.of("2020-08-27 03:38:02"));
  }

  @Test
  public void testMonthName() {
    String query =
        "source=people | eval `MONTHNAME(DATE('2020-08-26'))` = MONTHNAME(DATE('2020-08-26')) |"
            + " fields `MONTHNAME(DATE('2020-08-26'))`";
    testSimplePPL(query, List.of("Aug"));
  }

  @Test
  public void testLastDay() {
    String query =
        "source=people | eval `last_day('2023-02-06')` = last_day('2023-02-06') | fields"
            + " `last_day('2023-02-06')`";
    testSimplePPL(query, List.of("2023-02-28"));
  }

  @Test
  public void testDayName() {
    String query =
        "source=people | eval a = DAYNAME(DATE('2020-08-26')), b= DAYNAME(TIMESTAMP('2020-08-26"
            + " 12:00:00')), c=DAYNAME('2020-08-26')| fields a, b, c";
    testSimplePPL(query, List.of("wednesday", "wednesday", "wednesday"));
  }

  @Test
  public void testFromUnixTime() {
    String query =
        "source=people |  eval `FROM_UNIXTIME(1220249547)` = FROM_UNIXTIME(1220249547),"
            + " `FROM_UNIXTIME(1220249547, '%T')` = FROM_UNIXTIME(1220249547, '%T') | fields"
            + " `FROM_UNIXTIME(1220249547)`, `FROM_UNIXTIME(1220249547, '%T')`";
    testSimplePPL(query, List.of(1));
  }

  @Test
  public void testHour() {
    String query =
        "source=people | eval `HOUR(TIMESTAMP('2020-08-26 01:02:03'))` = HOUR(TIMESTAMP('2020-08-26"
            + " 01:02:03')) | fields `HOUR(TIMESTAMP('2020-08-26 01:02:03'))`";
    testSimplePPL(query, List.of(1));
  }

  private static String getUtcDate() {
    return LocalDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
  }

  @Test
  public void testDateAdd() {
    testSimplePPL(
        "source=test | eval `'2020-08-26' + 1h` = DATE_ADD(DATE('2020-08-26'), INTERVAL 1 HOUR) |"
            + " fields `'2020-08-26' + 1h`",
        List.of("2020-08-26 01:00:00"));
    testSimplePPL(
        "source=test | eval `ts '2020-08-26 01:01:01' + 1d` = DATE_ADD(TIMESTAMP('2020-08-26"
            + " 01:01:01'), INTERVAL 1 DAY) | fields `ts '2020-08-26 01:01:01' + 1d`",
        List.of("2020-08-27 01:01:01"));
    testSimplePPL(
        "source=test | eval `'2020-08-26' + 1h` = DATE_ADD('2020-08-26', INTERVAL 1 HOUR) | fields"
            + " `'2020-08-26' + 1h`",
        List.of("2020-08-26 01:00:00"));
    testSimplePPL(
        "source=test | eval `ts '2020-08-26 01:01:01' + 1d` = DATE_ADD('2020-08-26 01:01:01',"
            + " INTERVAL 1 DAY) | fields `ts '2020-08-26 01:01:01' + 1d`",
        List.of("2020-08-27 01:01:01"));
    testSimplePPL(
        "source=test | eval `ts '01:01:01' + 1h` = DATE_ADD('01:01:01',"
            + " INTERVAL 1 HOUR) | fields `ts '01:01:01' + 1h`",
        List.of(getUtcDate() + " 02:01:01"));
    testSimplePPL(
        "source=test | eval `ts '01:01:01' + 1d` = DATE_ADD(TIME('01:01:01'),"
            + " INTERVAL 1 HOUR) | fields `ts '01:01:01' + 1d`",
        List.of(getUtcDate() + " 02:01:01"));
  }

  @Test
  public void testDateSub() {
    testSimplePPL(
        "source=people | eval `'2008-01-02' - 31d` = DATE_SUB(DATE('2008-01-02'), INTERVAL 31 DAY),"
            + " `ts '2020-08-26 01:01:01' + 1h` = DATE_SUB(TIMESTAMP('2020-08-26 01:01:01'),"
            + " INTERVAL 1 HOUR) | fields `'2008-01-02' - 31d`, `ts '2020-08-26 01:01:01' + 1h`",
        List.of("2007-12-02 00:00:00", "2020-08-26 00:01:01"));
  }

  @Test
  public void testAddTime() {
    testSimplePPL(
        "source=people | eval `'2008-12-12' + 0` = ADDTIME(DATE('2008-12-12'), DATE('2008-11-15'))"
            + " | fields `'2008-12-12' + 0`",
        List.of("2008-12-12 00:00:00"));
    testSimplePPL(
        "source=people | eval `'23:59:59' + 0` = ADDTIME(TIME('23:59:59'), DATE('2004-01-01')) |"
            + " fields `'23:59:59' + 0`",
        List.of("23:59:59"));
    testSimplePPL(
        "source=people | eval `'2004-01-01' + '23:59:59'` = ADDTIME(DATE('2004-01-01'),"
            + " TIME('23:59:59')) | fields `'2004-01-01' + '23:59:59'`",
        List.of("2004-01-01 23:59:59"));
    testSimplePPL(
        "source=people | eval `'10:20:30' + '00:05:42'` = ADDTIME(TIME('10:20:30'),"
            + " TIME('00:05:42')) | fields `'10:20:30' + '00:05:42'`",
        List.of("10:26:12"));
    testSimplePPL(
        "source=people | eval `'2007-02-28 10:20:30' + '20:40:50'` = ADDTIME(TIMESTAMP('2007-02-28"
            + " 10:20:30'), TIMESTAMP('2002-03-04 20:40:50')) | fields `'2007-02-28 10:20:30' +"
            + " '20:40:50'`",
        List.of("2007-03-01 07:01:20"));
  }

  @Test
  public void testDayOfWeek() {
    testSimplePPL(
        "source=people | head 1 | eval `DAY_OF_WEEK(DATE('2020-08-26'))` ="
            + " DAY_OF_WEEK(DATE('2020-08-26')) | fields `DAY_OF_WEEK(DATE('2020-08-26'))`",
        List.of(4));
    testSimplePPL(
        "source=people | head 1 | eval `DAY_OF_WEEK(DATE('2020-08-26'))` ="
            + " DAY_OF_WEEK(TIMESTAMP('2020-08-26 13:04:12')) | fields"
            + " `DAY_OF_WEEK(DATE('2020-08-26'))`",
        List.of(4));
    testSimplePPL(
        "source=people | head 1 | eval `DAY_OF_WEEK(DATE('2020-08-26'))` ="
            + " DAY_OF_WEEK('2020-08-26') | fields `DAY_OF_WEEK(DATE('2020-08-26'))`",
        List.of(4));
    testSimplePPL(
        "source=people | head 1 | eval `DAY_OF_WEEK(DATE('2020-08-26'))` = DAY_OF_WEEK('2020-08-26"
            + " 13:04:12') | fields `DAY_OF_WEEK(DATE('2020-08-26'))`",
        List.of(4));
  }

  @Test
  public void testDayOfYear() {
    testSimplePPL(
        "source=people | eval `DAY_OF_YEAR(DATE('2020-08-26'))` = DAY_OF_YEAR(DATE('2020-08-26')) |"
            + " fields `DAY_OF_YEAR(DATE('2020-08-26'))`",
        List.of(239));
  }

  @Test
  public void testDateDiff() {
    testSimplePPL(
        "source=people | eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02"
            + " 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' - '2004-01-01'` ="
            + " DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')), `today - today` ="
            + " DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) | fields `'2000-01-02' -"
            + " '2000-01-01'`, `'2001-02-01' - '2004-01-01'`, `today - today`",
        List.of(1, -1064, 0));
  }

  @Test
  public void testTimestampAdd() {
    testSimplePPL(
            "source=people | eval `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')` = TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00') | eval `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')` = TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00') | fields `TIMESTAMPADD(DAY, 17, '2000-01-01 00:00:00')`, `TIMESTAMPADD(QUARTER, -1, '2000-01-01 00:00:00')`",
            List.of("2000-01-18 00:00:00", "1999-10-01 00:00:00"));
  }

  @Test
  public void testTimestampDiff() {
    testSimplePPL(
            "source=people | eval `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))` = TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00')) | fields `TIMESTAMPDIFF(SECOND, time('00:00:23'), time('00:00:00'))`",
            List.of(4, -23));
  }

  @Test
  public void testNow() {
    String execResult =
        execute(
            "source=people | eval `value_1` = NOW(), `value_2` = NOW() | fields `value_1`,"
                + " `value_2`");
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(2, dataRow.size());
    System.out.println(dataRow.get(0).getAsString());
    System.out.println(dataRow.get(1).getAsString());
    assertTrue(
        "Now must be of pattern yyyy-MM-dd HH:mm:ss",
        dataRow.get(0).getAsString().matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}"));
  }

  @Test
  public void testCurDate() {
    String execResult =
        execute("source=people | eval `CURDATE()` = CURDATE() | fields `CURDATE()`");
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(1, dataRow.size());
    assertTrue(
        "Result must be of pattern yyyy-MM-dd",
        dataRow.get(0).getAsString().matches("\\d{4}-\\d{2}-\\d{2}"));
    System.out.println(dataRow.get(0).getAsString());
  }

  @Test
  public void testTime() {
    testSimplePPL(
        "source=people | eval `TIME('13:49:00')` = TIME('13:49:00') | fields `TIME('13:49:00')`",
        List.of("13:49:00"));
    testSimplePPL(
        "source=people | eval `TIME('13:49')` = TIME('13:49') | fields `TIME('13:49')`",
        List.of("13:49:00"));
    testSimplePPL(
        "source=people | eval `TIME('2020-08-26 13:49:00')` = TIME('2020-08-26 13:49:00') | fields"
            + " `TIME('2020-08-26 13:49:00')`",
        List.of("13:49:00"));
    testSimplePPL(
        "source=people | eval `TIME('2020-08-26 13:49')` = TIME('2020-08-26 13:49') | fields"
            + " `TIME('2020-08-26 13:49')`",
        List.of("13:49:00"));
    testSimplePPL(
        "source=people | eval d = TIME(DATE('2020-08-26')) | fields d", List.of("00:00:00"));
    testSimplePPL(
        "source=people | eval t = TIME(TIMESTAMP('1998-01-31 13:14:15.012345')) | fields t",
        List.of("13:14:15"));
    testSimplePPL(
        "source=people | eval t = TIME(TIME('13:14:15.012345')) | fields t", List.of("13:14:15"));
  }

  @Test
  public void testDateFormat() {
    testSimplePPL(
        "source=people | eval `DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')` = "
            + "DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f'), `DATE_FORMAT"
            + "(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')`"
            + " = DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r') | fields "
            + "`DATE_FORMAT('1998-01-31 13:14:15.012345', '%T.%f')`, "
            + "`DATE_FORMAT(TIMESTAMP('1998-01-31 13:14:15.012345'), '%Y-%b-%D %r')`",
        List.of("13:14:15.012345", "1998-Jan-31st 01:14:15 PM"));
  }

  @Test
  public void testYear() {
    String query =
        "source=people | eval `YEAR(DATE('2020-08-26'))` = YEAR(DATE('2020-08-26')) | fields"
            + " `YEAR(DATE('2020-08-26'))`";
    testSimplePPL(query, List.of("2020"));
  }

  // String functions
  @Test
  public void testConcat() {
    String query =
        "source=test | eval `CONCAT('hello', 'world')` = CONCAT('hello', 'world'),"
            + " `CONCAT('hello ', 'whole ', 'world', '!')` = CONCAT('a', 'b ', 'c', 'd', 'e',"
            + " 'f', 'g', '1', '2') | fields `CONCAT('hello', 'world')`, `CONCAT('hello ',"
            + " 'whole ', 'world', '!')`";

    testSimplePPL(query, List.of("helloworld", "ab cdefg12"));
  }

  @Test
  public void testConcatWs() {
    String query =
        "source=test | eval `CONCAT_WS(',', 'hello', 'world')` = CONCAT_WS(',', 'hello',"
            + " 'world') | fields `CONCAT_WS(',', 'hello', 'world')`";
    testSimplePPL(query, List.of("hello,world"));
  }

  @Test
  public void testLength() {
    String query =
        "source=test | eval `LENGTH('helloworld')` = LENGTH('helloworld') | fields"
            + " `LENGTH('helloworld')`";
    testSimplePPL(query, List.of(10));
  }

  @Test
  public void testLower() {
    String query =
        "source=test | eval `LOWER('helloworld')` = LOWER('helloworld'), `LOWER('HELLOWORLD')`"
            + " = LOWER('HELLOWORLD') | fields `LOWER('helloworld')`, `LOWER('HELLOWORLD')`";
    testSimplePPL(query, List.of("helloworld", "helloworld"));
  }

  @Test
  public void testLtrim() {
    String query =
        "source=test | eval `LTRIM('   hello')` = LTRIM('   hello'), `LTRIM('hello   ')` ="
            + " LTRIM('hello   ') | fields `LTRIM('   hello')`, `LTRIM('hello   ')`";
    testSimplePPL(query, List.of("hello", "hello   "));
  }

  @Test
  public void testPosition() {
    String query =
        "source=test | eval `POSITION('world' IN 'helloworld')` = POSITION('world' IN"
            + " 'helloworld'), `POSITION('invalid' IN 'helloworld')`= POSITION('invalid' IN"
            + " 'helloworld')  | fields `POSITION('world' IN 'helloworld')`, `POSITION('invalid' IN"
            + " 'helloworld')`";
    testSimplePPL(query, List.of(6, 0));
  }

  @Test
  public void testReverse() {
    String query =
        "source=test | eval `REVERSE('abcde')` = REVERSE('abcde') | fields `REVERSE('abcde')`";
    testSimplePPL(query, List.of("edcba"));
  }

  // @Ignore
  @Test
  public void testRight() {
    List<Object> expected = new ArrayList<>();
    expected.add("world");
    expected.add("");
    String query =
        "source=test | eval `RIGHT('helloworld', 5)` = RIGHT('helloworld', 5), `RIGHT('HELLOWORLD',"
            + " 0)` = RIGHT('HELLOWORLD', 0) | fields `RIGHT('helloworld', 5)`,"
            + " `RIGHT('HELLOWORLD', 0)`";
    testSimplePPL(query, expected);
  }

  @Test
  public void testLike() {
    String query =
        "source="
            + TEST_INDEX_WILDCARD
            + " | WHERE Like(KeywordBody, '\\\\_test wildcard%') | fields KeywordBody";
  }

  @Test
  public void testRtrim() {
    String query =
        "source=test | eval `RTRIM('   hello')` = RTRIM('   hello'), `RTRIM('hello   ')` ="
            + " RTRIM('hello   ') | fields `RTRIM('   hello')`, `RTRIM('hello   ')`";
    testSimplePPL(query, List.of("   hello", "hello"));
  }

  @Test
  public void testSubstring() {
    String query =
        "source=test | eval `SUBSTRING('helloworld', 5)` = SUBSTRING('helloworld', 5),"
            + " `SUBSTRING('helloworld', 5, 3)` = SUBSTRING('helloworld', 5, 3) | fields"
            + " `SUBSTRING('helloworld', 5)`, `SUBSTRING('helloworld', 5, 3)`";
    testSimplePPL(query, List.of("oworld", "owo"));
  }

  @Test
  public void testTrim() {
    String query =
        "source=test | eval `TRIM('   hello')` = TRIM('   hello'), `TRIM('hello   ')` = TRIM('hello"
            + "   ') | fields `TRIM('   hello')`, `TRIM('hello   ')`";
    testSimplePPL(query, List.of("hello", "hello"));
  }

  @Test
  public void testUpper() {
    String query =
        "source=test | eval `UPPER('helloworld')` = UPPER('helloworld'), `UPPER('HELLOWORLD')` ="
            + " UPPER('HELLOWORLD') | fields `UPPER('helloworld')`, `UPPER('HELLOWORLD')`";
    testSimplePPL(query, List.of("HELLOWORLD", "HELLOWORLD"));
  }

  @Test
  public void testIf() {
    String actual =
        execute(
            "source=test_name_null | eval result = if(like(name, '%he%'), 'default', name) | fields"
                + " result");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"result\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  @Test
  public void testIfNull() {
    String actual =
        execute(
            "source=test_name_null | eval defaultName=ifnull(name, 'default') | fields"
                + " defaultName");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"defaultName\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"world\"\n"
            + "    ],\n"
            + "    [\n"
            + "      \"default\"\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  @Test
  public void testNullIf() {
    String actual =
        execute(
            "source=test_name_null | eval defaultName=nullif(name, 'world') | fields defaultName");
    assertEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"defaultName\",\n"
            + "      \"type\": \"string\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      \"hello\"\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ],\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 3,\n"
            + "  \"size\": 3\n"
            + "}",
        actual);
  }

  private static JsonArray parseAndGetFirstDataRow(String executionResult) {
    JsonObject sqrtResJson = JsonParser.parseString(executionResult).getAsJsonObject();
    JsonArray dataRows = sqrtResJson.getAsJsonArray("datarows");
    return dataRows.get(0).getAsJsonArray();
  }

  private void testSimplePPL(String query, List<Object> expectedValues) {
    String execResult = execute(query);
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(expectedValues.size(), dataRow.size());
    for (int i = 0; i < expectedValues.size(); i++) {
      Object expected = expectedValues.get(i);
      if (Objects.isNull(expected)) {
        Object actual = dataRow.get(i);
        assertNull(actual);
      } else if (expected instanceof BigDecimal) {
        Number actual = dataRow.get(i).getAsNumber();
        assertEquals(expected, actual);
      } else if (expected instanceof Double || expected instanceof Float) {
        Number actual = dataRow.get(i).getAsNumber();
        assertDoubleUlpEquals(((Number) expected).doubleValue(), actual.doubleValue(), 8);
      } else if (expected instanceof Long || expected instanceof Integer) {
        Number actual = dataRow.get(i).getAsNumber();
        assertEquals(((Number) expected).longValue(), actual.longValue());
      } else if (expected instanceof String) {
        String actual = dataRow.get(i).getAsString();
        assertEquals(expected, actual);
      } else if (expected instanceof Boolean) {
        Boolean actual = dataRow.get(i).getAsBoolean();
        assertEquals(expected, actual);
      } else {
        fail("Unsupported number type: " + expected.getClass().getName());
      }
    }
  }

  @Test
  public void testAbs() {
    String absPpl = "source=people | eval `ABS(-1)` = ABS(-1) | fields `ABS(-1)`";
    List<Object> expected = List.of(1);
    testSimplePPL(absPpl, expected);
  }

  @Test
  public void testAcos() {
    String acosPpl = "source=people | eval `ACOS(0)` = ACOS(0) | fields `ACOS(0)`";
    List<Object> expected = List.of(Math.PI / 2);
    testSimplePPL(acosPpl, expected);
  }

  @Test
  public void testAsin() {
    String asinPpl = "source=people | eval `ASIN(0)` = ASIN(0) | fields `ASIN(0)`";
    List<Object> expected = List.of(0.0);
    testSimplePPL(asinPpl, expected);
  }

  @Test
  public void testAtan() {
    // TODO: Error while preparing plan [LogicalProject(ATAN(2)=[ATAN(2)], ATAN(2, 3)=[ATAN(2, 3)])
    // ATAN defined in OpenSearch accepts single and double arguments, while that defined in SQL
    // standard library accepts only single argument.
    testSimplePPL(
        "source=people | eval `ATAN(2)` = ATAN(2), `ATAN(2, 3)` = ATAN(2, 3) | fields `ATAN(2)`,"
            + " `ATAN(2, 3)`",
        List.of(Math.atan(2), Math.atan2(2, 3)));
  }

  @Test
  public void testAtan2() {
    testSimplePPL(
        "source=people | eval `ATAN2(2, 3)` = ATAN2(2, 3) | fields `ATAN2(2, 3)`",
        List.of(Math.atan2(2, 3)));
  }

  @Test
  public void testCeiling() {
    testSimplePPL(
        "source=people | eval `CEILING(0)` = CEILING(0), `CEILING(50.00005)` = CEILING(50.00005),"
            + " `CEILING(-50.00005)` = CEILING(-50.00005) | fields `CEILING(0)`,"
            + " `CEILING(50.00005)`, `CEILING(-50.00005)`",
        List.of(Math.ceil(0.0), Math.ceil(50.00005), Math.ceil(-50.00005)));
    testSimplePPL(
        "source=people | eval `CEILING(3147483647.12345)` = CEILING(3147483647.12345),"
            + " `CEILING(113147483647.12345)` = CEILING(113147483647.12345),"
            + " `CEILING(3147483647.00001)` = CEILING(3147483647.00001) | fields"
            + " `CEILING(3147483647.12345)`, `CEILING(113147483647.12345)`,"
            + " `CEILING(3147483647.00001)`",
        List.of(
            Math.ceil(3147483647.12345),
            Math.ceil(113147483647.12345),
            Math.ceil(3147483647.00001)));
  }

  @Ignore
  @Test
  public void testConv() {
    // TODO: Error while preparing plan [LogicalProject(CONV('12', 10, 16)=[CONVERT('12', 10, 16)],
    // CONV('2C', 16, 10)=[CONVERT('2C', 16, 10)], CONV(12, 10, 2)=[CONVERT(12, 10, 2)], CONV(1111,
    // 2, 10)=[CONVERT(1111, 2, 10)])
    //  OpenSearchTableScan(table=[[OpenSearch, people]])
    String convPpl =
        "source=people | eval `CONV('12', 10, 16)` = CONV('12', 10, 16), `CONV('2C', 16, 10)` ="
            + " CONV('2C', 16, 10), `CONV(12, 10, 2)` = CONV(12, 10, 2), `CONV(1111, 2, 10)` ="
            + " CONV(1111, 2, 10) | fields `CONV('12', 10, 16)`, `CONV('2C', 16, 10)`, `CONV(12,"
            + " 10, 2)`, `CONV(1111, 2, 10)`";
    String execResult = execute(convPpl);
    JsonArray dataRow = parseAndGetFirstDataRow(execResult);
    assertEquals(4, dataRow.size());
    assertEquals("c", dataRow.get(0).getAsString());
    assertEquals("44", dataRow.get(1).getAsString());
    assertEquals("1100", dataRow.get(2).getAsString());
    assertEquals("15", dataRow.get(3).getAsString());
  }

  @Test
  public void testCos() {
    testSimplePPL("source=people | eval `COS(0)` = COS(0) | fields `COS(0)`", List.of(1.0));
  }

  @Test
  public void testCot() {
    testSimplePPL(
        "source=people | eval `COT(1)` = COT(1) | fields `COT(1)`", List.of(1.0 / Math.tan(1)));
  }

  @Test
  public void testCrc32() {
    // TODO: No corresponding built-in implementation
    testSimplePPL(
        "source=people | eval `CRC32('MySQL')` = CRC32('MySQL') | fields `CRC32('MySQL')`",
        List.of(3259397556L));
  }

  @Test
  public void testDegrees() {
    testSimplePPL(
        "source=people | eval `DEGREES(1.57)` = DEGREES(1.57) | fields `DEGREES(1.57)`",
        List.of(Math.toDegrees(1.57)));
  }

  @Test
  public void testEuler() {
    // TODO: No corresponding built-in implementation
    testSimplePPL("source=people | eval `E()` = E() | fields `E()`", List.of(Math.E));
  }

  @Test
  public void testExp() {
    testSimplePPL("source=people | eval `EXP(2)` = EXP(2) | fields `EXP(2)`", List.of(Math.exp(2)));
  }

  @Test
  public void testFloor() {
    testSimplePPL(
        "source=people | eval `FLOOR(0)` = FLOOR(0), `FLOOR(50.00005)` = FLOOR(50.00005),"
            + " `FLOOR(-50.00005)` = FLOOR(-50.00005) | fields `FLOOR(0)`, `FLOOR(50.00005)`,"
            + " `FLOOR(-50.00005)`",
        List.of(Math.floor(0.0), Math.floor(50.00005), Math.floor(-50.00005)));
    testSimplePPL(
        "source=people | eval `FLOOR(3147483647.12345)` = FLOOR(3147483647.12345),"
            + " `FLOOR(113147483647.12345)` = FLOOR(113147483647.12345), `FLOOR(3147483647.00001)`"
            + " = FLOOR(3147483647.00001) | fields `FLOOR(3147483647.12345)`,"
            + " `FLOOR(113147483647.12345)`, `FLOOR(3147483647.00001)`",
        List.of(
            Math.floor(3147483647.12345),
            Math.floor(113147483647.12345),
            Math.floor(3147483647.00001)));
    testSimplePPL(
        "source=people | eval `FLOOR(282474973688888.022)` = FLOOR(282474973688888.022),"
            + " `FLOOR(9223372036854775807.022)` = FLOOR(9223372036854775807.022),"
            + " `FLOOR(9223372036854775807.0000001)` = FLOOR(9223372036854775807.0000001) | fields"
            + " `FLOOR(282474973688888.022)`, `FLOOR(9223372036854775807.022)`,"
            + " `FLOOR(9223372036854775807.0000001)`",
        List.of(
            Math.floor(282474973688888.022),
            Math.floor(9223372036854775807.022),
            Math.floor(9223372036854775807.0000001)));
  }

  @Test
  public void testLn() {
    testSimplePPL("source=people | eval `LN(2)` = LN(2) | fields `LN(2)`", List.of(Math.log(2)));
  }

  @Test
  public void testLog() {
    // TODO: No built-in function for 2-operand log
    testSimplePPL(
        "source=people | eval `LOG(2)` = LOG(2), `LOG(2, 8)` = LOG(2, 8) | fields `LOG(2)`, `LOG(2,"
            + " 8)`",
        List.of(Math.log(2), Math.log(8) / Math.log(2)));
  }

  @Test
  public void testLog2() {
    testSimplePPL(
        "source=people | eval `LOG2(8)` = LOG2(8) | fields `LOG2(8)`",
        List.of(Math.log(8) / Math.log(2)));
  }

  @Test
  public void testLog10() {
    testSimplePPL(
        "source=people | eval `LOG10(100)` = LOG10(100) | fields `LOG10(100)`",
        List.of(Math.log10(100)));
  }

  @Test
  public void testMod() {
    // TODO: There is a difference between MOD in OpenSearch and SQL standard library
    // For MOD in Calcite, MOD(3.1, 2) = 1
    testSimplePPL(
        "source=people | eval `MOD(3, 2)` = MOD(3, 2), `MOD(3.1, 2)` = MOD(3.1, 2) | fields `MOD(3,"
            + " 2)`, `MOD(3.1, 2)`",
        List.of(1, 1.1));
  }

  @Test
  public void testPi() {
    testSimplePPL("source=people | eval `PI()` = PI() | fields `PI()`", List.of(Math.PI));
  }

  @Test
  public void testPowAndPower() {
    testSimplePPL(
        "source=people | eval `POW(3, 2)` = POW(3, 2), `POW(-3, 2)` = POW(-3, 2), `POW(3, -2)` ="
            + " POW(3, -2) | fields `POW(3, 2)`, `POW(-3, 2)`, `POW(3, -2)`",
        List.of(Math.pow(3, 2), Math.pow(-3, 2), Math.pow(3, -2)));
    testSimplePPL(
        "source=people | eval `POWER(3, 2)` = POWER(3, 2), `POWER(-3, 2)` = POWER(-3, 2), `POWER(3,"
            + " -2)` = POWER(3, -2) | fields `POWER(3, 2)`, `POWER(-3, 2)`, `POWER(3, -2)`",
        List.of(Math.pow(3, 2), Math.pow(-3, 2), Math.pow(3, -2)));
  }

  @Test
  public void testRadians() {
    testSimplePPL(
        "source=people | eval `RADIANS(90)` = RADIANS(90) | fields `RADIANS(90)`",
        List.of(Math.toRadians(90)));
  }

  @Test
  public void testRand() {
    String randPpl = "source=people | eval `RAND(3)` = RAND(3) | fields `RAND(3)`";
    String execResult1 = execute(randPpl);
    String execResult2 = execute(randPpl);
    assertEquals(execResult1, execResult2);
    double val = parseAndGetFirstDataRow(execResult1).get(0).getAsDouble();
    assertTrue(val >= 0 && val <= 1);
  }

  @Test
  public void testRound() {
    testSimplePPL(
        "source=people | eval `ROUND(12.34)` = ROUND(12.34), `ROUND(12.34, 1)` = ROUND(12.34, 1),"
            + " `ROUND(12.34, -1)` = ROUND(12.34, -1), `ROUND(12, 1)` = ROUND(12, 1) | fields"
            + " `ROUND(12.34)`, `ROUND(12.34, 1)`, `ROUND(12.34, -1)`, `ROUND(12, 1)`",
        List.of(
            Math.round(12.34),
            Math.round(12.34 * 10) / 10.0,
            Math.round(12.34 / 10) * 10.0,
            Math.round(12.0 * 10) / 10.0));
  }

  @Test
  public void testSign() {
    testSimplePPL(
        "source=people | eval `SIGN(1)` = SIGN(1), `SIGN(0)` = SIGN(0), `SIGN(-1.1)` = SIGN(-1.1) |"
            + " fields `SIGN(1)`, `SIGN(0)`, `SIGN(-1.1)`",
        List.of(1, 0, -1));
  }

  @Test
  public void testSin() {
    testSimplePPL(
        "source=people | eval `SIN(0)` = SIN(0) | fields `SIN(0)`", List.of(Math.sin(0.0)));
  }

  @Test
  public void testSqrt() {
    testSimplePPL(
        "source=people | eval `SQRT(4)` = SQRT(4), `SQRT(4.41)` = SQRT(4.41) | fields `SQRT(4)`,"
            + " `SQRT(4.41)`",
        List.of(Math.sqrt(4), Math.sqrt(4.41)));
  }

  @Test
  public void testCbrt() throws IOException {
    testSimplePPL(
        "source=people | eval `CBRT(8)` = CBRT(8), `CBRT(9.261)` = CBRT(9.261), `CBRT(-27)` ="
            + " CBRT(-27) | fields `CBRT(8)`, `CBRT(9.261)`, `CBRT(-27)`",
        List.of(Math.cbrt(8), Math.cbrt(9.261), Math.cbrt(-27)));
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
  }

  @Test
  public void testSqrtAndPow() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where sqrt(pow(age, 2)) = 30.0 | fields name, age",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(actual, schema("name", "string"), schema("age", "integer"));

    verifyDataRows(actual, rows("Hello", 30));
  }
}
