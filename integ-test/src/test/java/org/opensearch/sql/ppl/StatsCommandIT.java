/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class StatsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
  }

  @Test
  public void testStatsAvg() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats avg(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("avg(age)", null, "double"));
    verifyDataRows(response, rows(30.171D));
  }

  @Test
  public void testStatsSum() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats sum(balance)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("sum(balance)", null, "long"));
    verifyDataRows(response, rows(25714837));
  }

  @Test
  public void testStatsCount() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats count(account_number)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("count(account_number)", null, "integer"));
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsCountAll() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("count()", null, "integer"));
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsDistinctCount() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats distinct_count(gender)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("distinct_count(gender)", null, "integer"));
    verifyDataRows(response, rows(2));

    response = executeQuery(String.format("source=%s | stats dc(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("dc(age)", null, "integer"));
    verifyDataRows(response, rows(21));
  }

  @Test
  public void testStatsMin() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats min(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("min(age)", null, "long"));
    verifyDataRows(response, rows(20));
  }

  @Test
  public void testStatsMax() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats max(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("max(age)", null, "long"));
    verifyDataRows(response, rows(40));
  }

  @Test
  public void testStatsNested() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats avg(abs(age) * 2) as AGE", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("AGE", null, "double"));
    verifyDataRows(response, rows(60.342));
  }

  @Test
  public void testStatsNestedDoubleValue() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats avg(abs(age) * 2.0)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("avg(abs(age) * 2.0)", null, "double"));
    verifyDataRows(response, rows(60.342));
  }

  @Test
  public void testStatsWhere() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats sum(balance) as a by state | where a > 780000",
                TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("a", null, "long"), schema("state", null, "string"));
    verifyDataRows(response, rows(782199, "TX"));
  }

  @Test
  public void testGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "integer"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  // Todo. The column of agg function is in random order. This is because we create the project
  // all operator from the symbol table which can't maintain the original column order.
  @Test
  public void testMultipleAggregationFunction() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats min(age), max(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("min(age)", null, "long"), schema("max(age)", null, "long"));
    verifyDataRows(response, rows(20, 40));
  }

  @Test
  public void testStatsWithNull() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats avg(age)", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("avg(age)", null, "double"));
    verifyDataRows(response, rows(33.166666666666664));
  }

  @Test
  public void testStatsWithMissing() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats avg(balance)", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("avg(balance)", null, "double"));
    verifyDataRows(response, rows(31082.25));
  }

  @Test
  public void testStatsBySpan() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats count() by span(age,10)", TEST_INDEX_BANK));
    verifySchema(
        response, schema("count()", null, "integer"), schema("span(age,10)", null, "integer"));
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }

  @Test
  public void testStatsTimeSpan() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats count() by span(birthdate,1y)", TEST_INDEX_BANK));
    verifySchema(
        response,
        schema("count()", null, "integer"),
        schema("span(birthdate,1y)", null, "timestamp"));
    verifyDataRows(response, rows(2, "2017-01-01 00:00:00"), rows(5, "2018-01-01 00:00:00"));
  }

  @Test
  public void testStatsAliasedSpan() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by span(age,10) as age_bucket", TEST_INDEX_BANK));
    verifySchema(
        response, schema("count()", null, "integer"), schema("age_bucket", null, "integer"));
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }
}
