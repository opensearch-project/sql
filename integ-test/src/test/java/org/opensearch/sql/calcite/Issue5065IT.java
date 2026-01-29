/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration test for issue #5065: Calcite PPL doesn't handle array value columns if codegen
 * triggered.
 */
public class Issue5065IT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "test_idx_5065";

  @Override
  public void init() throws IOException {
    // Create test index without any explicit mapping first
    createIndexByRestClient(client(), TEST_INDEX, "{}");

    // Insert test document with array data
    Request request = new Request("POST", "/" + TEST_INDEX + "/_doc?refresh=true");
    request.setJsonEntity("{ \"nums\": [1, 2, 3] }");
    client().performRequest(request);
    
    // Now update the mapping to explicitly set nums as an array of longs
    // This should help OpenSearch recognize it as an array type
    String mapping =
        "{\n"
            + "  \"properties\": {\n"
            + "    \"nums\": {\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  }\n"
            + "}";
    
    Request mappingRequest = new Request("PUT", "/" + TEST_INDEX + "/_mapping");
    mappingRequest.setJsonEntity(mapping);
    client().performRequest(mappingRequest);
  }

  @After
  public void cleanup() throws IOException {
    Request request = new Request("DELETE", "/" + TEST_INDEX);
    client().performRequest(request);
  }

  @Test
  public void testExpandOnArrayField() throws IOException {
    // Execute query that triggers the bug
    String query = "source = " + TEST_INDEX + " | expand nums";
    
    try {
      JSONObject result = executeQuery(query);
      fail("Expected UnsupportedOperationException but query succeeded");
    } catch (Exception e) {
      // Expected: Clear error message about scalar types not supported
      String errorMessage = e.getMessage();
      assertTrue(
          "Error should mention expand only works on array types",
          errorMessage.contains("Expand command only works on array types")
              || errorMessage.contains("UnsupportedOperationException"));
      assertTrue(
          "Error should mention the field name",
          errorMessage.contains("nums") || errorMessage.contains("BIGINT"));
    }
  }
}
