/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.json.JSONObject;
import org.opensearch.rest.RestRequest;

/** Run QueryStringIT tests using http GET request */
public class QueryStringWithGetIT extends QueryStringIT {
  @Override
  public void init() throws IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }

  @Override
  public void mandatory_params_test() throws IOException {
    String query =
        "source="
            + TEST_INDEX_BEER
            + " | where query_string(['Tags' ^ 1.5, Title, 'Body' 4.2], 'taste')";
    JSONObject result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }
}
