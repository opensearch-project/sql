/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;

import java.io.IOException;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.monitor.GCedMemoryUsage;

public class ResourceMonitorIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.CLICK_BENCH);
  }

  @Test
  public void queryExceedResourceLimitShouldFail() throws IOException {
    // update plugins.ppl.query.memory_limit to 1%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "1%"));
    ResponseException exception = expectThrows(ResponseException.class, this::executeQuery);
    assertEquals(500, exception.getResponse().getStatusLine().getStatusCode());
    assertThat(exception.getMessage(), Matchers.containsString("Insufficient resources to"));
    assertThat(exception.getMessage(), Matchers.containsString("plugins.query.memory_limit"));

    // update plugins.ppl.query.memory_limit to default value 85%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "85%"));
    executeQuery();
  }

  private void executeQuery() throws IOException {
    if (GCedMemoryUsage.initialized()) {
      // ClickBench Q30 is a high memory consumption query. Run 5 times to ensure GC triggered.
      String query = sanitize(loadFromFile("clickbench/queries/q30.ppl"));
      for (int i = 0; i < 5; i++) {
        JSONObject result = executeQuery(query);
        verifyNumOfRows(result, 1);
      }
    } else {
      // Q2 is not a high memory consumption query.
      // It cannot run in 1% resource but passed in 85%.
      String query = sanitize(loadFromFile("clickbench/queries/q2.ppl"));
      JSONObject result = executeQuery(query);
      verifyNumOfRows(result, 1);
    }
  }
}
