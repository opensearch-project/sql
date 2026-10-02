/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.core.rest.RestStatus;

/** Tests that unsupported operations return proper 4xx client errors, not 5xx server errors. */
public class UnsupportedFunctionIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testUnsupportedFunctionReturns400NotHTTP200WithStatus500() throws IOException {
    // JSON_EXTRACT is a 3.x Calcite function unavailable in 2.x legacy engine
    try {
      executeQuery("SELECT JSON_EXTRACT(address, '$.name') FROM opensearch-sql_test_index_account");
      Assert.fail("Expected ResponseException, but none was thrown");
    } catch (ResponseException e) {
      assertThat(
          e.getResponse().getStatusLine().getStatusCode(),
          equalTo(RestStatus.BAD_REQUEST.getStatus()));
      final String entity = TestUtils.getResponseBody(e.getResponse());
      assertThat(entity, containsString("not supported in Schema"));
    }
  }
}
