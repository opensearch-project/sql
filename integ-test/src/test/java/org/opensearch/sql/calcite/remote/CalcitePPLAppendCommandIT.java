/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_WEBLOGS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAppendCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.WEBLOG);
  }

  @Test
  public void testAppend() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ source=%s |"
                    + " stats sum(age) as sum_age_by_state by state | sort sum_age_by_state ] |"
                    + " head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum_age_by_gender", "bigint"),
        schema("gender", "string"),
        schema("sum_age_by_state", "bigint"),
        schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, 369, "NV"),
        rows(null, null, 412, "NM"),
        rows(null, null, 414, "AZ"));
  }

  @Test
  public void testAppendEmptySearchCommand() throws IOException {
    List<String> emptySourcePPLs =
        Arrays.asList(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ |"
                    + " stats sum(age) as sum_age_by_state by state ]",
                TEST_INDEX_ACCOUNT),
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ ]",
                TEST_INDEX_ACCOUNT),
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | where age >"
                    + " 10 | append [ ] ]",
                TEST_INDEX_ACCOUNT),
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | where age >"
                    + " 10 | lookup %s gender as igender ]",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));

    for (String ppl : emptySourcePPLs) {
      JSONObject actual = executeQuery(ppl);
      verifySchemaInOrder(
          actual, schema("sum_age_by_gender", "bigint"), schema("gender", "string"));
      verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"));
    }
  }

  @Test
  public void testAppendEmptySearchWithJoin() throws IOException {
    withSettings(
        Settings.Key.CALCITE_SUPPORT_ALL_JOIN_TYPES,
        "true",
        () -> {
          List<String> emptySourceWithJoinPPLs =
              Arrays.asList(
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | "
                          + " join left=L right=R on L.gender = R.gender %s ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT),
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | "
                          + " cross join left=L right=R on L.gender = R.gender %s ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT),
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | "
                          + " left join left=L right=R on L.gender = R.gender %s ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT),
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | "
                          + " semi join left=L right=R on L.gender = R.gender %s ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT));

          for (String ppl : emptySourceWithJoinPPLs) {
            JSONObject actual = null;
            try {
              actual = executeQuery(ppl);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            verifySchemaInOrder(
                actual, schema("sum_age_by_gender", "bigint"), schema("gender", "string"));
            verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"));
          }

          List<String> emptySourceWithRightOrFullJoinPPLs =
              Arrays.asList(
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | where"
                          + " gender = 'F' |  right join on gender = gender [source=%s | stats"
                          + " count() as cnt by gender ] ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT),
                  String.format(
                      Locale.ROOT,
                      "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ | where"
                          + " gender = 'F' |  full join on gender = gender [source=%s | stats"
                          + " count() as cnt by gender ] ]",
                      TEST_INDEX_ACCOUNT,
                      TEST_INDEX_ACCOUNT));

          for (String ppl : emptySourceWithRightOrFullJoinPPLs) {
            JSONObject actual = null;
            try {
              actual = executeQuery(ppl);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
            verifySchemaInOrder(
                actual,
                schema("sum_age_by_gender", "bigint"),
                schema("gender", "string"),
                schema("cnt", "bigint"));
            verifyDataRows(
                actual,
                rows(14947, "F", null),
                rows(15224, "M", null),
                rows(null, "F", 493),
                rows(null, "M", 507));
          }
        });
  }

  @Test
  public void testAppendDifferentIndex() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender | append [ source=%s | stats"
                    + " sum(age) as bank_sum_age ]",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_BANK));
    verifySchemaInOrder(
        actual,
        schema("sum", "bigint"),
        schema("gender", "string"),
        schema("bank_sum_age", "bigint"));
    verifyDataRows(actual, rows(14947, "F", null), rows(15224, "M", null), rows(null, null, 238));
  }

  @Test
  public void testAppendWithMergedColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender |"
                    + " append [ source=%s | stats sum(age) as sum by state | sort sum ] | head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual, schema("sum", "bigint"), schema("gender", "string"), schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null),
        rows(15224, "M", null),
        rows(369, null, "NV"),
        rows(412, null, "NM"),
        rows(414, null, "AZ"));
  }

  @Test
  public void testAppendWithConflictTypeColumn() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        Locale.ROOT,
                        "source=%s | stats sum(age) as sum by gender | append [ source=%s | stats"
                            + " sum(age) as sum by state | sort sum | eval sum = cast(sum as"
                            + " double) ] | head 5",
                        TEST_INDEX_ACCOUNT,
                        TEST_INDEX_ACCOUNT)));

    assertTrue(
        "Error message should indicate type conflict",
        exception
            .getMessage()
            .contains("Unable to process column 'sum' due to incompatible types:"));
  }

  @Test
  public void testAppendSchemaMergeWithTimestampUDT() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | fields account_number, firstname | append [ source=%s | fields"
                    + " account_number, age, birthdate ] | where isnotnull(birthdate) and"
                    + " account_number > 30",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_BANK));
    verifySchemaInOrder(
        actual,
        schema("account_number", "bigint"),
        schema("firstname", "string"),
        schema("age", "int"),
        schema("birthdate", "timestamp"));
    verifyDataRows(actual, rows(32, null, 34, "2018-08-11 00:00:00"));
  }

  @Test
  public void testAppendSchemaMergeWithIpUDT() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | fields account_number, age | append [ source=%s | fields host ] |"
                    + " where cidrmatch(host, '0.0.0.0/24')",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_WEBLOGS));
    verifySchemaInOrder(
        actual, schema("account_number", "bigint"), schema("age", "bigint"), schema("host", "ip"));
    verifyDataRows(actual, rows(null, null, "0.0.0.2"));
  }
}
