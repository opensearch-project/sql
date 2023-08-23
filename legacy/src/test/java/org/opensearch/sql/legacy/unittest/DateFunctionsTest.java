/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest;

import static org.junit.Assert.assertTrue;
import static org.opensearch.search.builder.SearchSourceBuilder.ScriptField;
import static org.opensearch.sql.legacy.util.CheckScriptContents.getScriptFieldFromQuery;
import static org.opensearch.sql.legacy.util.CheckScriptContents.getScriptFilterFromQuery;
import static org.opensearch.sql.legacy.util.CheckScriptContents.scriptContainsString;
import static org.opensearch.sql.legacy.util.CheckScriptContents.scriptHasPattern;

import org.junit.BeforeClass;
import org.junit.Test;
import org.opensearch.sql.legacy.parser.ScriptFilter;
import org.opensearch.sql.legacy.parser.SqlParser;

public class DateFunctionsTest {

  private static SqlParser parser;

  @BeforeClass
  public static void init() {
    parser = new SqlParser();
  }

  /**
   * The following unit tests will only cover a subset of the available date functions as the
   * painless script is generated from the same template. More thorough testing will be done in
   * integration tests since output will differ for each function.
   */
  @Test
  public void yearInSelect() {
    String query = "SELECT YEAR(creationDate) " + "FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.year"));
  }

  @Test
  public void yearInWhere() {
    String query = "SELECT * " + "FROM dates " + "WHERE YEAR(creationDate) > 2012";
    ScriptFilter scriptFilter = getScriptFilterFromQuery(query, parser);
    assertTrue(scriptContainsString(scriptFilter, "doc['creationDate'].value.year"));
    assertTrue(scriptHasPattern(scriptFilter, "year_\\d+ > 2012"));
  }

  @Test
  public void weekOfYearInSelect() {
    String query = "SELECT WEEK_OF_YEAR(creationDate) " + "FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(
        scriptContainsString(
            scriptField, "doc['creationDate'].value.get(WeekFields.ISO.weekOfWeekBasedYear())"));
  }

  @Test
  public void weekOfYearInWhere() {
    String query = "SELECT * " + "FROM dates " + "WHERE WEEK_OF_YEAR(creationDate) > 15";
    ScriptFilter scriptFilter = getScriptFilterFromQuery(query, parser);
    assertTrue(
        scriptContainsString(
            scriptFilter, "doc['creationDate'].value.get(WeekFields.ISO.weekOfWeekBasedYear())"));
    assertTrue(scriptHasPattern(scriptFilter, "weekOfWeekyear_\\d+ > 15"));
  }

  @Test
  public void dayOfMonth() {
    String query = "SELECT DAY_OF_MONTH(creationDate) " + "FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.dayOfMonth"));
  }

  @Test
  public void hourOfDay() {
    String query = "SELECT HOUR_OF_DAY(creationDate) " + "FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.hour"));
  }

  @Test
  public void secondOfMinute() {
    String query = "SELECT SECOND_OF_MINUTE(creationDate) " + "FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.second"));
  }

  @Test
  public void month() {
    String query = "SELECT MONTH(creationDate) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.monthValue"));
  }

  @Test
  public void dayofmonth() {
    String query = "SELECT DAY_OF_MONTH(creationDate) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.dayOfMonth"));
  }

  @Test
  public void date() {
    String query = "SELECT DATE(creationDate) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(
        scriptContainsString(
            scriptField,
            "LocalDate.parse(doc['creationDate'].value.toString(),DateTimeFormatter.ISO_DATE_TIME)"));
  }

  @Test
  public void monthname() {
    String query = "SELECT MONTHNAME(creationDate) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "doc['creationDate'].value.month"));
  }

  @Test
  public void timestamp() {
    String query = "SELECT TIMESTAMP(creationDate) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(
        scriptContainsString(scriptField, "DateTimeFormatter.ofPattern('yyyy-MM-dd HH:mm:ss')"));
  }

  @Test
  public void maketime() {
    String query = "SELECT MAKETIME(1, 1, 1) FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(
        scriptContainsString(
            scriptField, "LocalTime.of(1, 1, 1).format(DateTimeFormatter.ofPattern('HH:mm:ss'))"));
  }

  @Test
  public void now() {
    String query = "SELECT NOW() FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "System.currentTimeMillis()"));
  }

  @Test
  public void curdate() {
    String query = "SELECT CURDATE() FROM dates";
    ScriptField scriptField = getScriptFieldFromQuery(query);
    assertTrue(scriptContainsString(scriptField, "System.currentTimeMillis()"));
  }
}
