/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.rest.RestRequest;

/** Run SimpleQueryStringIT tests using http GET request */
public class SimpleQueryStringWithGetIT extends SimpleQueryStringIT {
  @Override
  public void init() throws IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }

  @Test
  public void test_simple_query_string() throws IOException {
    String query =
        "SOURCE="
            + TEST_INDEX_BEER
            + " | WHERE simple_query_string(['Tags' ^ 1.5, Title, 'Body' 4.2], 'taste') |"
            + " fields Id";
    var result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }
}
