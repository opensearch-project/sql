/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

public class CrossClusterSearchIT extends PPLIntegTestCase {

  private final static String TEST_INDEX_BANK_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_BANK;
  private final static String TEST_INDEX_DOG_REMOTE = REMOTE_CLUSTER + ":" + TEST_INDEX_DOG;

  @Override
  public void init() throws IOException {
    configureMultiClusters();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK, remoteClient());
    loadIndex(Index.DOG);
    loadIndex(Index.DOG, remoteClient());
  }

  @Test
  public void testCrossClusterSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG_REMOTE));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testCrossClusterSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK_REMOTE));
    verifyDataRows(result, rows("Hattie"));
  }
}
