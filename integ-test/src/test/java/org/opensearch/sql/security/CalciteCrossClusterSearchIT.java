/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

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
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
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
  public void testCrossClusterRegexBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | regex firstname='.*att.*' | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testCrossClusterRegexWithNegation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | regex firstname!='.*att.*' | fields firstname",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result,
        rows("Virginia"),
        rows("Elinor"),
        rows("Dillard"),
        rows("Dale"),
        rows("Amber JOHnny"),
        rows("Nanette"));
  }

  @Test
  public void testCrossClusterRexBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname \\\"(?<initial>^[A-Z])\\\" | fields"
                    + " firstname, initial | head 3",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Amber JOHnny", "A"), rows("Hattie", "H"), rows("Nanette", "N"));
  }

  @Test
  public void testCrossClusterRexMultipleGroups() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=lastname \\\"(?<first>[A-Z])(?<rest>[a-z]+)\\\" |"
                    + " fields lastname, first, rest | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Duke Willmington", "D", "uke"), rows("Bond", "B", "ond"));
  }

  @Test
  public void testCrossClusterRexSedMode() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname mode=sed \\\"s/^[A-Z]/X/\\\" | fields"
                    + " firstname | head 3",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Xmber JOHnny"), rows("Xattie"), rows("Xanette"));
  }

  @Test
  public void testCrossClusterRexWithMaxMatch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=firstname \\\"(?<letter>[A-Z])\\\" max_match=2 |"
                    + " fields firstname, letter | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Amber JOHnny", new String[] {"A", "J"}), rows("Hattie", new String[] {"H"}));
  }

  @Test
  public void testCrossClusterRexWithOffsetField() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | rex field=lastname \\\"(?<vowel>[aeiou])\\\" offset_field=pos |"
                    + " fields lastname, vowel, pos | head 2",
                TEST_INDEX_BANK_REMOTE));
    verifyDataRows(
        result, rows("Duke Willmington", "u", "vowel=1-1"), rows("Bond", "o", "vowel=1-1"));
  }
}
