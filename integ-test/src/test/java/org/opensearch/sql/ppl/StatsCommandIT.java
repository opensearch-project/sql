/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class StatsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
  }

  @Test
  public void testStatsAvg() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats avg(age)"));
    verifySchema(response, schema("avg(age)", null, "double"));
    verifyDataRows(response, rows(30.171D));
  }

  @Test
  public void testStatsSum() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats sum(balance)"));
    verifySchema(response, schema("sum(balance)", null, "bigint"));
    verifyDataRows(response, rows(25714837));
  }

  @Test
  public void testStatsSumWithEnhancement() throws IOException {
    JSONObject response =
        executeQuery(
            Index.ACCOUNT.ppl(
                "stats sum(balance), sum(balance + 100), sum(balance - 100),"
                    + " sum(balance * 100), sum(balance / 100) by gender"));
    verifySchema(
        response,
        schema("sum(balance)", null, "bigint"),
        schema("sum(balance + 100)", null, "bigint"),
        schema("sum(balance - 100)", null, "bigint"),
        schema("sum(balance * 100)", null, "bigint"),
        schema("sum(balance / 100)", null, "bigint"),
        schema("gender", null, "string"));
    verifyDataRows(
        response,
        rows(12632310, 12681610, 12583010, 1263231000, 126080, "F"),
        rows(13082527, 13133227, 13031827, 1308252700, 130570, "M"));
  }

  @Test
  public void testStatsCount() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats count(account_number)"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("count(account_number)", null, "bigint"));
    } else {
      verifySchema(response, schema("count(account_number)", null, "int"));
    }
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsCountAll() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats count()"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("count()", null, "bigint"));
    } else {
      verifySchema(response, schema("count()", null, "int"));
    }
    verifyDataRows(response, rows(1000));

    response = executeQuery(Index.ACCOUNT.ppl("stats c()"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("c()", null, "bigint"));
    } else {
      verifySchema(response, schema("c()", null, "int"));
    }
    verifyDataRows(response, rows(1000));

    response = executeQuery(Index.ACCOUNT.ppl("stats count"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("count", null, "bigint"));
    } else {
      verifySchema(response, schema("count", null, "int"));
    }
    verifyDataRows(response, rows(1000));

    response = executeQuery(Index.ACCOUNT.ppl("stats c"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("c", null, "bigint"));
    } else {
      verifySchema(response, schema("c", null, "int"));
    }
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsCBy() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats c by gender"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("c", null, "bigint"), schema("gender", null, "string"));
    } else {
      verifySchema(response, schema("c", null, "int"), schema("gender", null, "string"));
    }
    verifyDataRows(response, rows(493, "F"), rows(507, "M"));
  }

  @Test
  public void testStatsDistinctCount() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats distinct_count(gender)"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("distinct_count(gender)", null, "bigint"));
    } else {
      verifySchema(response, schema("distinct_count(gender)", null, "int"));
    }
    verifyDataRows(response, rows(2));

    response = executeQuery(Index.ACCOUNT.ppl("stats dc(age)"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("dc(age)", null, "bigint"));
    } else {
      verifySchema(response, schema("dc(age)", null, "int"));
    }
    verifyDataRows(response, rows(21));
  }

  @Test
  public void testStatsMin() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats min(age)"));
    verifySchema(response, schema("min(age)", null, "bigint"));
    verifyDataRows(response, rows(20));
  }

  @Test
  public void testStatsMax() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats max(age)"));
    verifySchema(response, schema("max(age)", null, "bigint"));
    verifyDataRows(response, rows(40));
  }

  @Test
  public void testStatsNested() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats avg(abs(age) * 2) as AGE"));
    verifySchema(response, schema("AGE", null, "double"));
    verifyDataRows(response, rows(60.342));
  }

  @Test
  public void testStatsNestedDoubleValue() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats avg(abs(age) * 2.0)"));
    verifySchema(response, schema("avg(abs(age) * 2.0)", null, "double"));
    verifyDataRows(response, rows(60.342));
  }

  @Test
  public void testStatsWhere() throws IOException {
    JSONObject response =
        executeQuery(Index.ACCOUNT.ppl("stats sum(balance) as a by state | where a > 780000"));
    verifySchema(response, schema("a", null, "bigint"), schema("state", null, "string"));
    verifyDataRows(response, rows(782199, "TX"));
  }

  @Test
  public void testAvgGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats avg(balance) as a by age"));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testMinGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats min(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testMaxGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats max(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testSumGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats avg(balance) as a by age"));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testStddevSampGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats STDDEV_SAMP(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(null, 28),
        rows(null, 32),
        rows(null, 33),
        rows(null, 34),
        rows(null, 36));
  }

  @Test
  public void testStddevPopGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats STDDEV_POP(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(0, 28),
        rows(0, 32),
        rows(0, 33),
        rows(0, 34),
        rows(null, 36));
  }

  @Test
  public void testVarSampGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats VAR_SAMP(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(null, 28),
        rows(null, 32),
        rows(null, 33),
        rows(null, 34),
        rows(null, 36));
  }

  @Test
  public void testVarPopGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats VAR_POP(balance) as a by age"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(0, 28),
        rows(0, 32),
        rows(0, 33),
        rows(0, 34),
        rows(null, 36));
  }

  // Todo. The column of agg function is in random order. This is because we create the project
  // all operator from the symbol table which can't maintain the original column order.
  @Test
  public void testMultipleAggregationFunction() throws IOException {
    JSONObject response = executeQuery(Index.ACCOUNT.ppl("stats min(age), max(age)"));
    verifySchema(response, schema("min(age)", null, "bigint"), schema("max(age)", null, "bigint"));
    verifyDataRows(response, rows(20, 40));
  }

  @Test
  public void testStatsWithNull() throws IOException {
    JSONObject response = executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats avg(age)"));
    verifySchema(response, schema("avg(age)", null, "double"));
    verifyDataRows(response, rows(33.166666666666664));
  }

  @Test
  public void testSumWithNull() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("where age = 36 | stats sum(balance)"));
    verifySchema(response, schema("sum(balance)", null, "bigint"));
    // TODO: Fix -- temporary workaround for the pushdown issue:
    //  The current pushdown implementation will return 0 for sum when getting null values as input.
    //  Returning null should be the expected behavior.
    Integer expectedValue = isPushdownEnabled() ? 0 : null;
    verifyDataRows(response, rows(expectedValue));
  }

  @Test
  public void testStatsWithMissing() throws IOException {
    JSONObject response = executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats avg(balance)"));
    verifySchema(response, schema("avg(balance)", null, "double"));
    verifyDataRows(response, rows(31082.25));
  }

  @Test
  public void testStatsBySpan() throws IOException {
    JSONObject response = executeQuery(Index.BANK.ppl("stats count() by span(age,10)"));
    verifySchema(
        response,
        isCalciteEnabled() ? schema("count()", null, "bigint") : schema("count()", null, "int"),
        schema("span(age,10)", null, "int"));
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }

  @Test
  public void testStatsTimeSpan() throws IOException {
    JSONObject response = executeQuery(Index.BANK.ppl("stats count() by span(birthdate,1y)"));
    verifySchema(
        response,
        isCalciteEnabled() ? schema("count()", null, "bigint") : schema("count()", null, "int"),
        schema("span(birthdate,1y)", null, "timestamp"));
    verifyDataRows(response, rows(2, "2017-01-01 00:00:00"), rows(5, "2018-01-01 00:00:00"));
  }

  @Test
  public void testStatsAliasedSpan() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK.ppl("stats count() by span(age,10) as age_bucket"));
    if (isCalciteEnabled()) {
      verifySchema(response, schema("count()", null, "bigint"), schema("age_bucket", null, "int"));
    } else {
      verifySchema(response, schema("count()", null, "int"), schema("age_bucket", null, "int"));
    }
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }

  @Test
  public void testStatsBySpanAndMultipleFields() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK.ppl("stats count() by span(age,10), gender, state"));
    verifySchemaInOrder(
        response,
        isCalciteEnabled() ? schema("count()", null, "bigint") : schema("count()", null, "int"),
        schema("span(age,10)", null, "int"),
        schema("gender", null, "string"),
        schema("state", null, "string"));
    verifyDataRows(
        response,
        rows(1, 20, "F", "VA"),
        rows(1, 30, "F", "IN"),
        rows(1, 30, "F", "PA"),
        rows(1, 30, "M", "IL"),
        rows(1, 30, "M", "MD"),
        rows(1, 30, "M", "TN"),
        rows(1, 30, "M", "WA"));
  }

  @Test
  public void testStatsByMultipleFieldsAndSpan() throws IOException {
    // Use verifySchemaInOrder() and verifyDataRowsInOrder() to check that the span column is always
    // the first column in result whatever the order of span in query is first or last one
    JSONObject response =
        executeQuery(Index.BANK.ppl("stats count() by gender, state, span(age,10)"));
    verifySchemaInOrder(
        response,
        isCalciteEnabled() ? schema("count()", null, "bigint") : schema("count()", null, "int"),
        schema("span(age,10)", null, "int"),
        schema("gender", null, "string"),
        schema("state", null, "string"));
    verifyDataRows(
        response,
        rows(1, 20, "F", "VA"),
        rows(1, 30, "F", "IN"),
        rows(1, 30, "F", "PA"),
        rows(1, 30, "M", "IL"),
        rows(1, 30, "M", "MD"),
        rows(1, 30, "M", "TN"),
        rows(1, 30, "M", "WA"));
  }

  @Test
  public void testStatsPercentile() throws IOException {
    JSONObject response = executeQuery(Index.BANK.ppl("stats percentile(balance, 50)"));
    verifySchema(response, schema("percentile(balance, 50)", null, "bigint"));
    verifyDataRows(response, rows(32838));
  }

  @Test
  public void testStatsPercentileWithNull() throws IOException {
    JSONObject response =
        executeQuery(Index.BANK_WITH_NULL_VALUES.ppl("stats percentile(balance, 50)"));
    verifySchema(response, schema("percentile(balance, 50)", null, "bigint"));
    verifyDataRows(response, rows(39225));
  }

  @Test
  public void testStatsPercentileWithCompression() throws IOException {
    JSONObject response = executeQuery(Index.BANK.ppl("stats percentile(balance, 50, 20)"));
    verifySchema(response, schema("percentile(balance, 50, 20)", null, "bigint"));
    verifyDataRows(response, rows(32838));

    // disable pushdown by adding a eval command
    JSONObject responsePushdownDisabled =
        executeQuery(Index.BANK.ppl("eval a = 1 | stats percentile(balance, 50, 20)"));
    verifySchema(responsePushdownDisabled, schema("percentile(balance, 50, 20)", null, "bigint"));
    verifyDataRows(responsePushdownDisabled, rows(32838));
  }

  @Test
  public void testStatsPercentileWhere() throws IOException {
    JSONObject response =
        executeQuery(
            Index.BANK.ppl("stats percentile(balance, 50) as p50 by state | where p50 > 40000"));
    verifySchema(response, schema("p50", null, "bigint"), schema("state", null, "string"));
    verifyDataRows(response, rows(48086, "IN"), rows(40540, "PA"));
  }

  @Test
  public void testStatsPercentileByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            Index.BANK_WITH_NULL_VALUES.ppl("stats percentile(balance, 50) as p50 by age"));
    verifySchema(response, schema("p50", null, "bigint"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(isCalciteEnabled() && !isPushdownEnabled() ? null : 0, null),
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(isCalciteEnabled() && !isPushdownEnabled() ? null : 0, 36));
  }

  @Test
  public void testStatsPercentileBySpan() throws IOException {
    JSONObject response =
        executeQuery(
            Index.BANK.ppl("stats percentile(balance, 50) as p50 by span(age, 10) as age_bucket"));
    verifySchema(response, schema("p50", null, "bigint"), schema("age_bucket", null, "int"));
    verifyDataRows(response, rows(32838, 20), rows(39225, 30));
  }
}
