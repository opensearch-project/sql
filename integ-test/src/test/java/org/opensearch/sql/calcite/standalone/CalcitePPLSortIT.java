/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;

@Ignore
public class CalcitePPLSortIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testFieldsAndSort1() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields + firstname, gender, account_number | sort - account_number",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("firstname", "string"),
        schema("gender", "string"),
        schema("account_number", "long"));
    verifyDataRows(
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
  public void testFieldsAndSort2() {
    JSONObject actual =
        executeQuery(String.format("source=%s | fields age | sort - age", TEST_INDEX_BANK));
    verifySchema(actual, schema("age", "integer"));
    verifyDataRows(actual, rows(39), rows(36), rows(36), rows(34), rows(33), rows(32), rows(28));
  }

  @Test
  public void testFieldsAndSortTwoFields() {
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
        schema("account_number", "long"));
    verifyDataRows(
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
  public void testFieldsAndSortWithDescAndLimit() {
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
        schema("account_number", "long"));
    verifyDataRows(
        actual,
        rows("Dillard", "F", 32),
        rows("Virginia", "F", 25),
        rows("Nanette", "F", 13),
        rows("Elinor", "M", 20),
        rows("Dale", "M", 18));
  }

  @Test
  public void testSortAccountAndFieldsAccount() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number", TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "long"));
    verifyDataRows(actual, rows(32), rows(25), rows(20), rows(18), rows(13), rows(6), rows(1));
  }

  @Test
  public void testSortAccountAndFieldsNameAccount() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields firstname, account_number",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("account_number", "long"));
    verifyDataRows(
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
  public void testSortAccountAndFieldsAccountName() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - account_number | fields account_number, firstname",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("account_number", "long"), schema("firstname", "string"));
    verifyDataRows(
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
  public void testSortAgeAndFieldsAge() {
    JSONObject actual =
        executeQuery(String.format("source=%s | sort - age | fields age", TEST_INDEX_BANK));
    verifySchema(actual, schema("age", "integer"));
    verifyDataRows(actual, rows(39), rows(36), rows(36), rows(34), rows(33), rows(32), rows(28));
  }

  @Test
  public void testSortAgeAndFieldsNameAge() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | sort - age | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(
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
  public void testSortAgeNameAndFieldsNameAge() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | sort - age, - firstname | fields firstname, age", TEST_INDEX_BANK));
    verifySchema(actual, schema("firstname", "string"), schema("age", "integer"));
    verifyDataRows(
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
    verifyOrder(
        result,
        rows("Dale", 4180),
        rows("Nanette", 32838),
        rows("Amber JOHnny", 39225),
        rows("Dillard", 48086),
        rows("Hattie", null),
        rows("Elinor", null),
        rows("Virginia", null));
  }
}
