/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class PaginationIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.CALCS);
    loadIndex(Index.ONLINE);
  }

  @Test
  public void testSmallDataSet() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_CALCS;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    assertEquals(4, response.getInt("size"));
    TestUtils.verifyIsV2Cursor(response);
  }

  @Test
  public void testLargeDataSetV1() throws IOException {
    var v1query = "SELECT * from " + TEST_INDEX_ONLINE + " WHERE 1 = 1";
    var v1response = new JSONObject(executeFetchQuery(v1query, 4, "jdbc"));
    assertEquals(4, v1response.getInt("size"));
    TestUtils.verifyIsV1Cursor(v1response);
  }

  @Test
  public void testLargeDataSetV2() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_ONLINE;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertEquals(4, response.getInt("size"));
    TestUtils.verifyIsV2Cursor(response);
  }
}
