/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class ConvertTZFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void inRangeZeroToPositive() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2008-05-15 12:00:00','+00:00','+10:00')");
    verifySchema(
        result, schema("convert_tz('2008-05-15 12:00:00','+00:00','+10:00')", null, "timestamp"));
    verifyDataRows(result, rows("2008-05-15 22:00:00"));
  }

  @Test
  public void inRangeNegativeZeroToPositiveZero() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 00:00:00','-00:00','+00:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 00:00:00','-00:00','+00:00')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 00:00:00"));
  }

  @Test
  public void inRangePositiveToPositive() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 00:00:00','+10:00','+11:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 00:00:00','+10:00','+11:00')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 01:00:00"));
  }

  @Test
  public void inRangeNegativeToPositive() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','-08:00','+09:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','-08:00','+09:00')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-13 04:34:50"));
  }

  @Test
  public void inRangeSameTimeZone() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','+09:00','+09:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','+09:00','+09:00')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 11:34:50"));
  }

  @Test
  public void inRangeTwentyFourHourTimeOffset() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','-12:00','+12:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','-12:00','+12:00')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-13 11:34:50"));
  }

  @Test
  public void inRangeFifteenMinuteTimeZones() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 13:00:00','+09:30','+05:45')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 13:00:00','+09:30','+05:45')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 09:15:00"));
  }

  @Test
  public void inRangeRandomTimes() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 13:00:00','+09:31','+05:11')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 13:00:00','+09:31','+05:11')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 08:40:00"));
  }

  @Test
  public void nullField2Under() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-30 11:34:50','-14:00','+08:00')");
    verifySchema(
        result, schema("convert_tz('2021-05-30 11:34:50','-14:00','+08:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullField3Over() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','-12:00','+14:01')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','-12:00','+14:01')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void inRangeMinOnPoint() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 15:00:00','-13:59','-13:59')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 15:00:00','-13:59','-13:59')", null, "timestamp"));
    verifyDataRows(result, rows("2021-05-12 15:00:00"));
  }

  // Invalid is any invalid input in a field. In the timezone fields it also includes all
  // non-timezone characters including `****` as well as `+10:0` which is missing an extra
  // value on the end to make it `HH:mm` timezone.
  // Invalid input returns null.
  @Test
  public void nullField3InvalidInput() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','+10:0','+14:01')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','+10:0','+14:01')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullField2InvalidInput() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-05-12 11:34:50','+14:01','****')");
    verifySchema(
        result, schema("convert_tz('2021-05-12 11:34:50','+14:01','****')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  // Invalid input in the timestamp field of CONVERT_TZ results in a null field. It is any input
  // which is not of the format `yyyy-MM-dd HH:mm:ss`
  @Test
  public void nulltimestampInvalidInput() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021----','+00:00','+00:00')");
    verifySchema(result, schema("convert_tz('2021----','+00:00','+00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueFebruary() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-02-30 10:00:00','+00:00','+00:00')");
    verifySchema(
        result, schema("convert_tz('2021-02-30 10:00:00','+00:00','+00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueApril() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-04-31 10:00:00','+00:00','+00:00')");
    verifySchema(
        result, schema("convert_tz('2021-04-31 10:00:00','+00:00','+00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueMonth() throws IOException {
    var result = executeJdbcRequest("SELECT convert_tz('2021-13-03 10:00:00','+00:00','+00:00')");
    verifySchema(
        result, schema("convert_tz('2021-13-03 10:00:00','+00:00','+00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }
}
