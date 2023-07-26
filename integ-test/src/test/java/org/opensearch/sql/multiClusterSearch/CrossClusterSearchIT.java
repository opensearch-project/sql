/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.multiClusterSearch;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CrossClusterSearchIT extends PPLIntegTestCase {

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  private final static String TEST_INDEX_BANK_REMOTE = MULTI_REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  private final static String TEST_INDEX_DOG_REMOTE = MULTI_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private final static String TEST_INDEX_DOG_MATCH_ALL_REMOTE = MATCH_ALL_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private final static String TEST_INDEX_ACCOUNT_REMOTE = MULTI_REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;

  @Override
  public void init() throws IOException {
    configureMultiClusters(MULTI_REMOTE_CLUSTER);
    loadIndex(Index.BANK);
    loadIndex(Index.BANK, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    loadIndex(Index.ACCOUNT, remoteClient());
  }

  @Test
  public void testCrossClusterSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testMatchAllCrossClusterSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG_MATCH_ALL_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testCrossClusterSearchWithoutLocalFieldMappingShouldFail() throws IOException {
    exceptionRule.expect(ResponseException.class);
    exceptionRule.expectMessage("400 Bad Request");
    exceptionRule.expectMessage("IndexNotFoundException");

    executeQuery(String.format("search source=%s", TEST_INDEX_ACCOUNT_REMOTE));
  }

  @Test
  public void testCrossClusterSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result = executeQuery(String.format(
        "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterSearchMultiClusters() throws IOException {
    JSONObject result = executeQuery(String.format(
        "search source=%s,%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK_REMOTE, TEST_INDEX_BANK));
    verifyDataRows(result,
        rows("Hattie"),
        rows("Hattie"));
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
        columnName("IS_GENERATEDCOLUMN")
    );
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
        columnName("IS_GENERATEDCOLUMN")
    );
  }
}
