/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.util.TestUtils.getResponseBody;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

// regression test for mismatched status codes bug -- invoking unsupported functions should return
// 400 statuses.
public class UnsupportedFunctionErrorIT extends SQLIntegTestCase {

  @Test
  public void jsonExtractReturns400NotServerError() throws IOException {
    Request createIndex = new Request("PUT", "/json_test");
    performRequest(client(), createIndex);

    Request indexDoc = new Request("POST", "/json_test/_doc?refresh=true");
    indexDoc.setJsonEntity("{\"message\": \"{\\\"name\\\": \\\"test\\\", \\\"value\\\": 123}\"}");
    performRequest(client(), indexDoc);

    // If JSON_EXTRACT is later added to the sql engine, fix this test by changing to a dummy
    // function name
    ResponseException exception =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("SELECT JSON_EXTRACT(message, '$.name') FROM json_test"));

    // V2358402234: UnsupportedOperationException should return 400 (client error) not 500
    assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());

    String body = getResponseBody(exception.getResponse());
    org.json.JSONObject response = new org.json.JSONObject(body);

    // Verify JSON error structure
    assertTrue("Response should have 'error' field", response.has("error"));
    assertTrue("Response should have 'status' field", response.has("status"));
    assertEquals("Status in body should match HTTP status", 400, response.getInt("status"));

    org.json.JSONObject error = response.getJSONObject("error");
    assertTrue("Error should have 'type' field", error.has("type"));
    assertTrue("Error should have 'details' field", error.has("details"));

    String details = error.getString("details");
    assertTrue(
        "Error details should mention JSON_EXTRACT or unsupported",
        details.contains("JSON_EXTRACT") || details.contains("not supported"));
  }
}
