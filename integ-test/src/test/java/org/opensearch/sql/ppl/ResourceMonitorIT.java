/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;

public class ResourceMonitorIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DOG);
  }

  @Test
  public void queryExceedResourceLimitShouldFail() throws IOException {
    // update plugins.ppl.query.memory_limit to 1%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "1%"));
    String query = String.format("search source=%s age=20", TEST_INDEX_DOG);

    ResponseException exception = expectThrows(ResponseException.class, () -> executeQuery(query));
    assertEquals(500, exception.getResponse().getStatusLine().getStatusCode());
    assertThat(
        exception.getMessage(),
        Matchers.containsString("resource is not enough to run the" + " query, quit."));

    // update plugins.ppl.query.memory_limit to default value 85%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "85%"));
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }
}
