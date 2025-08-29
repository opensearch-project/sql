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
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Cross Cluster Search tests with Calcite enabled for enhanced fields features. */
public class CalciteCrossClusterSearchIT extends PPLIntegTestCase {

  static {
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
  private static final String TEST_INDEX_ACCOUNT_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_ACCOUNT;
  private static final String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;
  private static final String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
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
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.ACCOUNT, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.TIME_TEST_DATA, remoteClient());
    enableCalcite();
  }

  @Test
  public void testCrossClusterFieldsSpaceDelimited() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("search source=%s | fields dog_name age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"));
    verifySchema(result, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsWildcardPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields dog*", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"));
    verifySchema(result, schema("dog_name", "string"));
  }

  @Test
  public void testCrossClusterFieldsWildcardSuffix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields *Name", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"));
    verifySchema(result, schema("dog_name", "string"), schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterFieldsMixedDelimiters() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | fields dog_name, age holdersName", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"), columnName("holdersName"));
    verifySchema(
        result,
        schema("dog_name", "string"),
        schema("age", "bigint"),
        schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterTableCommand() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | table dog_name age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("age"));
    verifySchema(result, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsAllWildcard() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields *", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
    verifySchema(
        result,
        schema("dog_name", "string"),
        schema("holdersName", "string"),
        schema("age", "bigint"));
  }

  @Test
  public void testCrossClusterFieldsExclusion() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | fields - age", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"));
    verifySchema(result, schema("dog_name", "string"), schema("holdersName", "string"));
  }

  @Test
  public void testCrossClusterTableWildcardPrefix() throws IOException {
    JSONObject result =
        executeQuery(String.format("search source=%s | table first*", TEST_INDEX_BANK_REMOTE));
    verifyColumn(result, columnName("firstname"));
    verifySchema(result, schema("firstname", "string"));
  }

  @Test
  public void testCrossClusterFieldsAndTableEquivalence() throws IOException {
    JSONObject fieldsResult =
        executeQuery(
            String.format("search source=%s | fields dog_name age", TEST_INDEX_DOG_REMOTE));
    JSONObject tableResult =
        executeQuery(String.format("search source=%s | table dog_name age", TEST_INDEX_DOG_REMOTE));

    verifyColumn(fieldsResult, columnName("dog_name"), columnName("age"));
    verifyColumn(tableResult, columnName("dog_name"), columnName("age"));
    verifySchema(fieldsResult, schema("dog_name", "string"), schema("age", "bigint"));
    verifySchema(tableResult, schema("dog_name", "string"), schema("age", "bigint"));
  }

  @Test
  public void testDefaultBinCrossCluster() throws IOException {
    // Default bin without any parameters
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testSpanBinCrossCluster() throws IOException {
    // Span-based binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age span=10 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testCountBinCrossCluster() throws IOException {
    // Count-based binning (bins parameter)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age bins=5 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testMinSpanBinCrossCluster() throws IOException {
    // MinSpan-based binning
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age minspan=5 start=0 end=100 | stats count() by age | sort age |"
                    + " head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(451L, "20-30"), rows(504L, "30-40"), rows(45L, "40-50"));
  }

  @Test
  public void testRangeBinCrossCluster() throws IOException {
    // Range-based binning (start/end only)
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | bin age start=0 end=100 | stats count() by age | sort age | head 3",
                TEST_INDEX_ACCOUNT_REMOTE));
    verifySchema(result, schema("count()", null, "bigint"), schema("age", null, "string"));

    verifyDataRows(result, rows(1000L, "0-100"));
  }

  @Test
  public void testTimeBinCrossCluster() throws IOException {
    // Time-based binning with span
    JSONObject result =
        executeQuery(
            REMOTE_CLUSTER
                + ":opensearch-sql_test_index_time_data"
                + " | bin @timestamp span=1h"
                + " | fields `@timestamp`, value | sort `@timestamp` | head 3");
    verifySchema(result, schema("@timestamp", null, "timestamp"), schema("value", null, "int"));

    // With 1-hour spans
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", 8945),
        rows("2025-07-28 01:00:00", 7623),
        rows("2025-07-28 02:00:00", 9187));
  }
}
