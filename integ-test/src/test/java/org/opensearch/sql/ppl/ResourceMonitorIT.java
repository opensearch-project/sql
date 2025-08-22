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
    // ClickBench Q30 is high memory consumption query.
    String query = sanitize(loadFromFile("clickbench/queries/q30.ppl"));

    ResponseException exception =
        expectThrows(ResponseException.class, () -> executeQueryMultipleTimes(query));
    assertEquals(500, exception.getResponse().getStatusLine().getStatusCode());
    assertThat(
        exception.getMessage(),
        Matchers.containsString("insufficient resources to run the query, quit."));

    // update plugins.ppl.query.memory_limit to default value 85%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "85%"));
    executeQueryMultipleTimes(query);
  }

  /** Run the same query multiple times in case GC can be triggered in integration test */
  private void executeQueryMultipleTimes(String query) throws IOException {
    for (int i = 0; i < 10; i++) {
      JSONObject result = executeQuery(query);
      verifyNumOfRows(result, 1);
    }
  }
}
