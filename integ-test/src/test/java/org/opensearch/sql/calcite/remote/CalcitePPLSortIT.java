/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLSortIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testFieldsAndSort1() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort - account_number",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("gender", "string"),
        schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        actual,
        rows("Dillard", "F", 32),
        rows("Virginia", "F", 25),
        rows("Elinor", "M", 20),
        rows("Dale", "M", 18),
        rows("Nanette", "F", 13),
        rows("Hattie", "M", 6),
        rows("Amber JOHnny", "M", 1));
  }

  @Test
  public void testFieldsAndSort2() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | fields age | sort - age", TEST_INDEX_BANK));
    verifySchema(actual, schema("age", "int"));
    verifyDataRowsInOrder(
        actual, rows(39), rows(36), rows(36), rows(34), rows(33), rows(32), rows(28));
  }

  @Test
  public void testFieldsAndSortTwoFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort + gender, -"
                    + " account_number",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("gender", "string"),
        schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        actual,
        rows("Dillard", "F", 32),
        rows("Virginia", "F", 25),
        rows("Nanette", "F", 13),
        rows("Elinor", "M", 20),
        rows("Dale", "M", 18),
        rows("Hattie", "M", 6),
        rows("Amber JOHnny", "M", 1));
  }

  @Test
  public void testFieldsAndSortWithDescAndLimit() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort + gender, -"
                    + " account_number | head 5",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("gender", "string"),
        schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        actual,
        rows("Dillard", "F", 32),
        rows("Virginia", "F", 25),
        rows("Nanette", "F", 13),
        rows("Elinor", "M", 20),
        rows("Dale", "M", 18));
  }

  @Test
  public void testSortAccountAndFieldsAccount() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number", TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        actual, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testSortAccountAndFieldsNameAccount() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields firstname, account_number",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("account_number", "bigint"));
    verifyDataRowsInOrder(
        actual,
        rows("Dillard", 32),
        rows("Virginia", 25),
        rows("Elinor", 20),
        rows("Dale", 18),
        rows("Nanette", 13),
        rows("Hattie", 6),
        rows("Amber JOHnny", 1));
  }

  @Test
  public void testSortAccountAndFieldsAccountName() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        actual,
        rows(32, "Dillard"),
        rows(25, "Virginia"),
        rows(20, "Elinor"),
        rows(18, "Dale"),
        rows(13, "Nanette"),
        rows(6, "Hattie"),
        rows(1, "Amber JOHnny"));
  }

  @Test
  public void testSortAgeAndFieldsAge() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | sort - age | fields age", TEST_INDEX_BANK));
    verifySchema(actual, schema("age", "int"));
    verifyDataRowsInOrder(
        actual, rows(39), rows(36), rows(36), rows(34), rows(33), rows(32), rows(28));
  }

  @Test
  public void testSortAgeAndFieldsNameAge() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | sort - age | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRowsInOrder(
        actual,
        rows("Virginia", 39),
        rows("Hattie", 36),
        rows("Elinor", 36),
        rows("Dillard", 34),
        rows("Dale", 33),
        rows("Amber JOHnny", 32),
        rows("Nanette", 28));
  }

  @Test
  public void testSortAgeNameAndFieldsNameAge() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - age, - firstname | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "int"));
    verifyDataRowsInOrder(
        actual,
        rows("Virginia", 39),
        rows("Hattie", 36),
        rows("Elinor", 36),
        rows("Dillard", 34),
        rows("Dale", 33),
        rows("Amber JOHnny", 32),
        rows("Nanette", 28));
  }

  @Test
  public void testSortWithNullValue() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort balance | fields firstname, balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRowsInOrder(
        result,
        rows("Hattie", null),
        rows("Elinor", null),
        rows("Virginia", null),
        rows("Dale", 4180),
        rows("Nanette", 32838),
        rows("Amber JOHnny", 39225),
        rows("Dillard", 48086));
  }

  @Test
  public void testSortDate() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort birthdate | fields firstname, birthdate", TEST_INDEX_BANK));
    verifySchema(result, schema("firstname", "string"), schema("birthdate", "timestamp"));
    verifyDataRowsInOrder(
        result,
        rows("Amber JOHnny", "2017-10-23 00:00:00"),
        rows("Hattie", "2017-11-20 00:00:00"),
        rows("Nanette", "2018-06-23 00:00:00"),
        rows("Elinor", "2018-06-27 00:00:00"),
        rows("Dillard", "2018-08-11 00:00:00"),
        rows("Virginia", "2018-08-19 00:00:00"),
        rows("Dale", "2018-11-13 23:33:20"));
  }

  @Test
  public void testSortWithAscKeyword() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number asc | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(1, "Amber JOHnny"),
        rows(6, "Hattie"),
        rows(13, "Nanette"),
        rows(18, "Dale"),
        rows(20, "Elinor"),
        rows(25, "Virginia"),
        rows(32, "Dillard"));
  }

  @Test
  public void testSortWithAKeyword() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number a | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(1, "Amber JOHnny"),
        rows(6, "Hattie"),
        rows(13, "Nanette"),
        rows(18, "Dale"),
        rows(20, "Elinor"),
        rows(25, "Virginia"),
        rows(32, "Dillard"));
  }

  @Test
  public void testSortExprRespectsSystemQuerySizeLimit() throws IOException {
    withSettings(
        Settings.Key.QUERY_SIZE_LIMIT,
        "50",
        () -> {
          JSONObject result;
          try {
            result =
                executeQuery(
                    String.format(
                        "source=%s | eval sort_expr = age + 1 | sort sort_expr | fields"
                            + " firstname",
                        TEST_INDEX_ACCOUNT));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          verifyNumOfRows(result, 50);
        });
  }

  @Test
  public void testSortWithDescKeyword() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort account_number desc | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(
        result,
        rows(32, "Dillard"),
        rows(25, "Virginia"),
        rows(20, "Elinor"),
        rows(18, "Dale"),
        rows(13, "Nanette"),
        rows(6, "Hattie"),
        rows(1, "Amber JOHnny"));
  }

  @Test
  public void testSortWithCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort 3 account_number | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(result, schema("account_number", "bigint"), schema("firstname", "string"));
    verifyDataRowsInOrder(result, rows(1, "Amber JOHnny"), rows(6, "Hattie"), rows(13, "Nanette"));
  }

  @Test
  public void testSortWithStrCast() throws IOException {

    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | sort STR(account_number) | fields account_number", TEST_INDEX_BANK));
    verifyDataRowsInOrder(
        result, rows(1), rows(13), rows(18), rows(20), rows(25), rows(32), rows(6));
  }

  @Test
  public void testSortWithAutoCast() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | sort AUTO(age) | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(result, schema("firstname", "string"), schema("age", "int"));
    verifyDataRowsInOrder(
        result,
        rows("Nanette", 28),
        rows("Amber JOHnny", 32),
        rows("Dale", 33),
        rows("Dillard", 34),
        rows("Hattie", 36),
        rows("Elinor", 36),
        rows("Virginia", 39));
  }
}
