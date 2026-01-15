/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;

/** Cross Cluster Search tests to be executed with security plugin. */
public class CrossClusterSearchIT extends CrossClusterTestBase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testCrossClusterSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testMatchAllCrossClusterSearchAllFields() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s", TEST_INDEX_DOG_MATCH_ALL_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testCrossClusterSearchWithoutLocalFieldMappingShouldFail() throws IOException {
    var exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery(String.format("search source=%s", TEST_INDEX_ACCOUNT_REMOTE)));
    assertTrue(exception.getMessage().contains("IndexNotFoundException"));
  }

  @Test
  public void testCrossClusterSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterSearchMultiClusters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s,%s firstname='Hattie' | fields firstname",
                TEST_INDEX_BANK_REMOTE, TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Hattie"));
  }

  @Test
  public void testCrossClusterDescribeAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN"));
  }

  @Test
  public void testMatchAllCrossClusterDescribeAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("describe %s", TEST_INDEX_DOG_MATCH_ALL_REMOTE));
    verifyColumn(
        result,
        columnName("TABLE_CAT"),
        columnName("TABLE_SCHEM"),
        columnName("TABLE_NAME"),
        columnName("COLUMN_NAME"),
        columnName("DATA_TYPE"),
        columnName("TYPE_NAME"),
        columnName("COLUMN_SIZE"),
        columnName("BUFFER_LENGTH"),
        columnName("DECIMAL_DIGITS"),
        columnName("NUM_PREC_RADIX"),
        columnName("NULLABLE"),
        columnName("REMARKS"),
        columnName("COLUMN_DEF"),
        columnName("SQL_DATA_TYPE"),
        columnName("SQL_DATETIME_SUB"),
        columnName("CHAR_OCTET_LENGTH"),
        columnName("ORDINAL_POSITION"),
        columnName("IS_NULLABLE"),
        columnName("SCOPE_CATALOG"),
        columnName("SCOPE_SCHEMA"),
        columnName("SCOPE_TABLE"),
        columnName("SOURCE_DATA_TYPE"),
        columnName("IS_AUTOINCREMENT"),
        columnName("IS_GENERATEDCOLUMN"));
  }

  @Test
  public void testCrossClusterSortWithCount() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort 1 age | fields firstname, age", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Nanette", 28));
  }

  @Test
  public void testCrossClusterSortWithDesc() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort age desc | fields firstname", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result,
        rows("Virginia"),
        rows("Hattie"),
        rows("Elinor"),
        rows("Dillard"),
        rows("Dale"),
        rows("Amber JOHnny"),
        rows("Nanette"));
  }

  @Test
  public void testCrossClusterSortWithTypeCasting() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | sort num(account_number) | fields account_number",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows(1), rows(6), rows(13), rows(18), rows(20), rows(25), rows(32));
  }

  @Test
  public void testCrossClusterPercentileShortcuts() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | stats perc50(balance), p95(balance)", TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("perc50(balance)"), columnName("p95(balance)"));
  }

  @Test
  public void testCrossClusterMultiMatchWithoutFields() throws IOException {
    // Test multi_match without fields parameter on remote cluster
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where multi_match('Hattie') | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterSimpleQueryStringWithoutFields() throws IOException {
    // Test simple_query_string without fields parameter on remote cluster
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where simple_query_string('Hattie') | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterQueryStringWithoutFields() throws IOException {
    // Test query_string without fields parameter on remote cluster
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where query_string('Hattie') | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterAddTotals() throws IOException {
    // Test query_string without fields parameter on remote cluster
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s| sort 1 age | fields firstname, age | addtotals age",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Nanette", 28, 28));
  }

  /** CrossClusterSearchIT Test for addcoltotals. */
  @Test
  public void testCrossClusterAddColTotals() throws IOException {
    // Test query_string without fields parameter on remote cluster
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where  firstname='Hattie' or firstname ='Nanette'|fields"
                    + " firstname,age,balance | addcoltotals age balance",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Hattie", 36, 5686), rows("Nanette", 28, 32838), rows(null, 64, 38524));
  }

  @Test
  public void testCrossClusterAppend() throws IOException {
    // TODO: We should enable calcite by default in CrossClusterSearchIT?
    enableCalcite();

    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | stats count() as cnt by gender | append [ search source=%s |"
                    + " stats count() as cnt ]",
                TEST_INDEX_BANK_REMOTE, TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows(3, "F"), rows(4, "M"), rows(7, null));

    disableCalcite();
  }

  @Test
  public void testCrossClusterConvert() throws IOException {
    enableCalcite();

    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) | fields balance",
                TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("balance"));

    disableCalcite();
  }

  @Test
  public void testCrossClusterConvertWithAlias() throws IOException {
    enableCalcite();

    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | convert auto(balance) AS balance_num | fields balance_num",
                TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("balance_num"));

    disableCalcite();
  }
}
