/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BEER;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.rest.RestRequest;

/** Run MultiMatchIT tests using http GET request */
public class MultiMatchWithGetIT extends MultiMatchIT {
  @Override
  public void init() throws IOException {
    super.init();
    this.method = RestRequest.Method.GET;
  }

  @Test
  public void test_multi_match() throws IOException {
    String query =
        "SOURCE="
            + TEST_INDEX_BEER
            + " | WHERE multi_match(['Tags' ^ 1.5, Title, 'Body' 4.2], 'taste') | fields Id";
    var result = executeQuery(query);
    assertEquals(16, result.getInt("total"));
  }
}
