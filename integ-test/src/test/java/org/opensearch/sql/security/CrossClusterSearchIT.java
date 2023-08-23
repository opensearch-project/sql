/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Cross Cluster Search tests to be executed with security plugin. */
public class CrossClusterSearchIT extends PPLIntegTestCase {

  static {
    // find a remote cluster
    String[] clusterNames = System.getProperty("cluster.names").split(",");
    var remote = "remoteCluster";
    for (var cluster : clusterNames) {
      if (cluster.startsWith("remote")) {
        remote = cluster;
        break;
      }
    }
    REMOTE_CLUSTER = remote;
  }

  public static final String REMOTE_CLUSTER;

  private static final String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  private static final String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private static final String TEST_INDEX_DOG_MATCH_ALL_REMOTE =
      MATCH_ALL_REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private static final String TEST_INDEX_ACCOUNT_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;

  private static boolean initialized = false;

  @SneakyThrows
  @BeforeEach
  public void initialize() {
    if (!initialized) {
      setUpIndices();
      initialized = true;
    }
  }

  @Override
  protected void init() throws Exception {
    configureMultiClusters(REMOTE_CLUSTER);
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
    assertTrue(
        exception.getMessage().contains("IndexNotFoundException")
            && exception.getMessage().contains("400 Bad Request"));
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
}
