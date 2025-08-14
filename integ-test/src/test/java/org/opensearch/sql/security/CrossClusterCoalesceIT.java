/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** Cross Cluster Search tests for enhanced coalesce function with Calcite. */
public class CrossClusterCoalesceIT extends PPLIntegTestCase {

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
    enableCalcite();
    configureMultiClusters(REMOTE_CLUSTER);
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
  }

  @Test
  public void testCrossClusterCoalesceBasic() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(holdersName, dog_name, 'unknown') |"
                    + " fields dog_name, holdersName, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceMixedTypes() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(age, holdersName, 'fallback') | fields"
                    + " age, holdersName, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("age"), columnName("holdersName"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceNonExistentFields() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce(missing_field, dog_name) | fields"
                    + " dog_name, result | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("result"));
  }

  @Test
  public void testCrossClusterCoalesceEmptyString() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval result = coalesce('', dog_name) | fields dog_name, result"
                    + " | head 1",
                TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("result"));
  }
}
