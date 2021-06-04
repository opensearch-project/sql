/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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

    ResponseException exception =
        expectThrows(ResponseException.class, () -> executeQuery(query));
    assertEquals(503, exception.getResponse().getStatusLine().getStatusCode());
    assertThat(exception.getMessage(), Matchers.containsString("resource is not enough to run the"
        + " query, quit."));

    // update plugins.ppl.query.memory_limit to default value 85%
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_MEMORY_LIMIT.getKeyValue(), "85%"));
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }
}
