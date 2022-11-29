/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

import java.io.IOException;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_MULTI_NESTED;

public class NestedIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.MULTI_NESTED);
  }

  @Test
  public void nested_string_subfield_test() {
    String query = "SELECT nested(message.info) FROM " + TEST_INDEX_MULTI_NESTED;
    JSONObject result = executeJdbcRequest(query);
    assertEquals(6, result.getInt("total"));
  }
}
