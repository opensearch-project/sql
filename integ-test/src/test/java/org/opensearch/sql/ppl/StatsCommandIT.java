/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATE_NULL;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;

public class StatsCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.BANK);
    loadIndex(Index.TIME_TEST_DATA_WITH_NULL);
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
    verifySchema(response, schema("sum(balance)", null, "bigint"));
    verifyDataRows(response, rows(25714837));
  }

  @Test
  public void testStatsSumWithEnhancement() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats sum(balance), sum(balance + 100), sum(balance - 100),"
                    + " sum(balance * 100), sum(balance / 100) by gender",
                TEST_INDEX_ACCOUNT));
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
    JSONObject response =
        executeQuery(String.format("source=%s | stats count(account_number)", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("count(account_number)", null, "bigint"));

    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsCountAll() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats count()", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("count()", null, "bigint"));
    verifyDataRows(response, rows(1000));

    response = executeQuery(String.format("source=%s | stats c()", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("c()", null, "bigint"));
    verifyDataRows(response, rows(1000));

    response = executeQuery(String.format("source=%s | stats count", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("count", null, "bigint"));
    verifyDataRows(response, rows(1000));

    response = executeQuery(String.format("source=%s | stats c", TEST_INDEX_ACCOUNT));

    verifySchema(response, schema("c", null, "bigint"));
    verifyDataRows(response, rows(1000));
  }

  @Test
  public void testStatsCBy() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats c by gender", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("c", null, "bigint"), schema("gender", null, "string"));
    verifyDataRows(response, rows(493, "F"), rows(507, "M"));
  }

  @Test
  public void testStatsDistinctCount() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats distinct_count(gender)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("distinct_count(gender)", null, "bigint"));
    verifyDataRows(response, rows(2));

    response = executeQuery(String.format("source=%s | stats dc(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("dc(age)", null, "bigint"));
    verifyDataRows(response, rows(21));
  }

  @Test
  public void testStatsMin() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats min(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("min(age)", null, "bigint"));
    verifyDataRows(response, rows(20));
  }

  @Test
  public void testStatsMax() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats max(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("max(age)", null, "bigint"));
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
    verifySchema(response, schema("a", null, "bigint"), schema("state", null, "string"));
    verifyDataRows(response, rows(782199, "TX"));
  }

  @Test
  public void testAvgGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testAvgGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false avg(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testMinGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats min(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testMinGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false min(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        response,
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testMaxGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats max(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testMaxGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false max(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        response,
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testSumGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testSumGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false avg(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(32838D, 28),
        rows(39225D, 32),
        rows(4180D, 33),
        rows(48086D, 34),
        rows(null, 36));
  }

  @Test
  public void testStatsWithLimit() throws IOException {
    // The original rows count is 6 if no head 5. See the test `testSumGroupByNullValue`.
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age | head 5",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    // If push down disabled, the final results will no longer be stable. In DSL, the order is
    // guaranteed because we always sort by bucket field, while we don't add sort in the plan.
    if (!isPushdownDisabled()) {
      verifyDataRows(
          response,
          rows(null, null),
          rows(32838D, 28),
          rows(39225D, 32),
          rows(4180D, 33),
          rows(48086D, 34));
    } else {
      assert ((Integer) response.get("size") == 5);
    }

    response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age | head 5 | head 2 from 1",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    if (!isPushdownDisabled()) {
      verifyDataRows(response, rows(32838D, 28), rows(39225D, 32));
    } else {
      assert ((Integer) response.get("size") == 2);
    }

    response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age | sort - age | head 5 | head 2 from 1",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    if (!isPushdownDisabled()) {
      verifyDataRows(response, rows(48086D, 34), rows(4180D, 33));
    } else {
      assert ((Integer) response.get("size") == 2);
    }

    response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age | sort - a | head 5 | head 2 from 1",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
    if (!isPushdownDisabled()) {
      verifyDataRows(response, rows(39225D, 32), rows(32838D, 28));
    } else {
      assert ((Integer) response.get("size") == 2);
    }
  }

  @Test
  public void testStddevSampGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats STDDEV_SAMP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testStddevSampGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false STDDEV_SAMP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        response, rows(null, 28), rows(null, 32), rows(null, 33), rows(null, 34), rows(null, 36));
  }

  @Test
  public void testStddevPopGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats STDDEV_POP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testStddevPopGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false STDDEV_POP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(response, rows(0, 28), rows(0, 32), rows(0, 33), rows(0, 34), rows(null, 36));
  }

  @Test
  public void testVarSampGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats VAR_SAMP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testVarSampGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false VAR_SAMP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        response, rows(null, 28), rows(null, 32), rows(null, 33), rows(null, 34), rows(null, 36));
  }

  @Test
  public void testVarPopGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats VAR_POP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
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
  public void testVarPopGroupByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false VAR_POP(balance) as a by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(response, rows(0, 28), rows(0, 32), rows(0, 33), rows(0, 34), rows(null, 36));
  }

  // Todo. The column of agg function is in random order. This is because we create the project
  // all operator from the symbol table which can't maintain the original column order.
  @Test
  public void testMultipleAggregationFunction() throws IOException {
    JSONObject response =
        executeQuery(String.format("source=%s | stats min(age), max(age)", TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("min(age)", null, "bigint"), schema("max(age)", null, "bigint"));
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
  public void testStatsWithNullNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false avg(age)",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("avg(age)", null, "double"));
    verifyDataRows(response, rows(33.166666666666664));
  }

  @Test
  public void testSumWithNull() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where age = 36 | stats sum(balance)",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("sum(balance)", null, "bigint"));
    // TODO: Fix -- temporary workaround for the pushdown issue:
    //  The current pushdown implementation will return 0 for sum when getting null values as input.
    //  Returning null should be the expected behavior.
    Integer expectedValue = isPushdownDisabled() ? null : 0;
    verifyDataRows(response, rows(expectedValue));
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
    verifySchema(response, schema("count()", null, "bigint"), schema("span(age,10)", null, "int"));
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }

  @Test
  public void testStatsTimeSpan() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats count() by span(birthdate,1y)", TEST_INDEX_BANK));
    verifySchema(
        response,
        schema("count()", null, "bigint"),
        schema("span(birthdate,1y)", null, "timestamp"));
    verifyDataRows(response, rows(2, "2017-01-01 00:00:00"), rows(5, "2018-01-01 00:00:00"));
  }

  @Test
  public void testStatsAliasedSpan() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by span(age,10) as age_bucket", TEST_INDEX_BANK));

    verifySchema(response, schema("count()", null, "bigint"), schema("age_bucket", null, "int"));
    verifyDataRows(response, rows(1, 20), rows(6, 30));
  }

  @Test
  public void testStatsBySpanAndMultipleFields() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by span(age,10), gender, state", TEST_INDEX_BANK));
    verifySchemaInOrder(
        response,
        schema("count()", null, "bigint"),
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
        executeQuery(
            String.format(
                "source=%s | stats count() by gender, state, span(age,10)", TEST_INDEX_BANK));
    verifySchemaInOrder(
        response,
        schema("count()", null, "bigint"),
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
    JSONObject response =
        executeQuery(String.format("source=%s | stats percentile(balance, 50)", TEST_INDEX_BANK));
    verifySchema(response, schema("percentile(balance, 50)", null, "bigint"));
    verifyDataRows(response, rows(32838));
  }

  @Test
  public void testStatsPercentileWithNull() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats percentile(balance, 50)", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("percentile(balance, 50)", null, "bigint"));
    verifyDataRows(response, rows(39225));
  }

  @Test
  public void testStatsPercentileWithCompression() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats percentile(balance, 50, 20)", TEST_INDEX_BANK));
    verifySchema(response, schema("percentile(balance, 50, 20)", null, "bigint"));
    verifyDataRows(response, rows(32838));

    // disable pushdown by adding a eval command
    JSONObject responsePushdownDisabled =
        executeQuery(
            String.format(
                "source=%s | eval a = 1 | stats percentile(balance, 50, 20)", TEST_INDEX_BANK));
    verifySchema(responsePushdownDisabled, schema("percentile(balance, 50, 20)", null, "bigint"));
    verifyDataRows(responsePushdownDisabled, rows(32838));
  }

  @Test
  public void testStatsPercentileWhere() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats percentile(balance, 50) as p50 by state | where p50 > 40000",
                TEST_INDEX_BANK));
    verifySchema(response, schema("p50", null, "bigint"), schema("state", null, "string"));
    verifyDataRows(response, rows(48086, "IN"), rows(40540, "PA"));
  }

  @Test
  public void testStatsPercentileByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats percentile(balance, 50) as p50 by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("p50", null, "bigint"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(isPushdownDisabled() ? null : 0, null),
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(isPushdownDisabled() ? null : 0, 36));
  }

  @Test
  public void testStatsPercentileByNullValueNonNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats bucket_nullable=false percentile(balance, 50) as p50 by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("p50", null, "bigint"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(isPushdownDisabled() ? null : 0, 36));
  }

  @Test
  public void testStatsPercentileBySpan() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats percentile(balance, 50) as p50 by span(age, 10) as age_bucket",
                TEST_INDEX_BANK));
    verifySchema(response, schema("p50", null, "bigint"), schema("age_bucket", null, "int"));
    verifyDataRows(response, rows(32838, 20), rows(39225, 30));
  }

  @Test
  public void testDisableLegacyPreferred() throws IOException {
    withSettings(
        Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED,
        "false",
        () -> {
          JSONObject response = null;
          try {
            response =
                executeQuery(
                    String.format(
                        "source=%s | stats avg(balance) as a by age",
                        TEST_INDEX_BANK_WITH_NULL_VALUES));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
          verifyDataRows(
              response,
              rows(32838D, 28),
              rows(39225D, 32),
              rows(4180D, 33),
              rows(48086D, 34),
              rows(null, 36));
        });
  }

  @Test
  public void testStatsBySpanTimeWithNullBucket() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats percentile(value, 50) as p50 by span(@timestamp, 12h) as"
                    + " half_day",
                TEST_INDEX_TIME_DATE_NULL));
    verifySchema(response, schema("p50", null, "int"), schema("half_day", null, "timestamp"));
    verifyDataRows(
        response,
        rows(8523, "2025-07-28 00:00:00"),
        rows(8094, "2025-07-28 12:00:00"),
        rows(8429, "2025-07-29 00:00:00"),
        rows(8216, "2025-07-29 12:00:00"),
        rows(8493, "2025-07-30 00:00:00"),
        rows(8426, "2025-07-30 12:00:00"),
        rows(8213, "2025-07-31 00:00:00"),
        rows(8490, "2025-07-31 12:00:00"));
  }

  @Test
  public void testStatsByCounts() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval b_1 = balance + 1 | stats count(), count() as c1,"
                    + " count(account_number), count(lastname) as c2, count(balance/10),"
                    + " count(pow(balance, 2)) as c3, count(b_1) by gender",
                TEST_INDEX_ACCOUNT));
    verifySchema(
        response,
        schema("count()", null, "bigint"),
        schema("c1", null, "bigint"),
        schema("count(account_number)", null, "bigint"),
        schema("c2", null, "bigint"),
        schema("count(balance/10)", null, "bigint"),
        schema("c3", null, "bigint"),
        schema("count(b_1)", null, "bigint"),
        schema("gender", null, "string"));
    verifyDataRows(
        response,
        rows(493, 493, 493, 493, 493, 493, 493, "F"),
        rows(507, 507, 507, 507, 507, 507, 507, "M"));
  }
}
