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

public class DateTimeImplementationIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void inRangeZeroToStringTZ() throws IOException {
    var result =
        executeJdbcRequest("SELECT DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')");
    verifySchema(
        result,
        schema("DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')", null, "timestamp"));
    verifyDataRows(result, rows("2008-12-24 21:30:00"));
  }

  @Test
  public void inRangeZeroToPositive() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-12-25 05:30:00+00:00', '+01:00')");
    verifySchema(
        result, schema("DATETIME('2008-12-25 05:30:00+00:00', '+01:00')", null, "timestamp"));
    verifyDataRows(result, rows("2008-12-25 06:30:00"));
  }

  @Test
  public void inRangeNegativeToPositive() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-12-25 05:30:00-05:00', '+05:00')");
    verifySchema(
        result, schema("DATETIME('2008-12-25 05:30:00-05:00', '+05:00')", null, "timestamp"));
    verifyDataRows(result, rows("2008-12-25 15:30:00"));
  }

  @Test
  public void inRangeTwentyHourOffset() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2004-02-28 23:00:00-10:00', '+10:00')");
    verifySchema(
        result, schema("DATETIME('2004-02-28 23:00:00-10:00', '+10:00')", null, "timestamp"));
    verifyDataRows(result, rows("2004-02-29 19:00:00"));
  }

  @Test
  public void inRangeYearChange() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+10:00', '-10:00')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+10:00', '-10:00')", null, "timestamp"));
    verifyDataRows(result, rows("2007-12-31 06:00:00"));
  }

  @Test
  public void inRangeZeroNoToTZ() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+10:00')");
    verifySchema(result, schema("DATETIME('2008-01-01 02:00:00+10:00')", null, "timestamp"));
    verifyDataRows(result, rows("2008-01-01 02:00:00"));
  }

  @Test
  public void inRangeZeroNoTZ() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00')");
    verifySchema(result, schema("DATETIME('2008-01-01 02:00:00')", null, "timestamp"));
    verifyDataRows(result, rows("2008-01-01 02:00:00"));
  }

  @Test
  public void inRangeZeroDayConvert() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+12:00', '-12:00')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+12:00', '-12:00')", null, "timestamp"));
    verifyDataRows(result, rows("2007-12-31 02:00:00"));
  }

  @Test
  public void inRangeJustInRangeNegative() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+10:00', '-13:59')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+10:00', '-13:59')", null, "timestamp"));
    verifyDataRows(result, rows("2007-12-31 02:01:00"));
  }

  @Test
  public void inRangeJustInRangePositive() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+14:00', '-10:00')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+14:00', '-10:00')", null, "timestamp"));
    verifyDataRows(result, rows("2007-12-31 02:00:00"));
  }

  @Test
  public void nullField3Under() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+10:00', '-14:01')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+10:00', '-14:01')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullField1Over() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2008-01-01 02:00:00+14:01', '-10:00')");
    verifySchema(
        result, schema("DATETIME('2008-01-01 02:00:00+14:01', '-10:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueFebruary() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2021-02-30 10:00:00')");
    verifySchema(result, schema("DATETIME('2021-02-30 10:00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueApril() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2021-04-31 10:00:00')");
    verifySchema(result, schema("DATETIME('2021-04-31 10:00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }

  @Test
  public void nullDateTimeInvalidDateValueMonth() throws IOException {
    var result = executeJdbcRequest("SELECT DATETIME('2021-13-03 10:00:00')");
    verifySchema(result, schema("DATETIME('2021-13-03 10:00:00')", null, "timestamp"));
    verifyDataRows(result, rows(new Object[] {null}));
  }
}
