/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.opensearch.client.ResponseException;

/** Tests to cover requests with "?format=csv" parameter */
public class GetEndpointQueryIT extends SQLIntegTestCase {

  @Rule public ExpectedException rule = ExpectedException.none();

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void getEndPointShouldBeInvalid() throws IOException {
    rule.expect(ResponseException.class);
    rule.expectMessage("Incorrect HTTP method");
    String query = "select name from " + TEST_INDEX_ACCOUNT;
    executeQueryWithGetRequest(query);
  }
}
