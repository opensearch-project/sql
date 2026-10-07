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

    // ponytail: regression test for V2358402234
    // Status code is correct (400), but response format is broken (plain text not JSON)
    // AOSS 2.17: {"error": {...}, "status": 500} → JSON + wrong status
    // Current 3.x: "The following method is not supported..." → plain text + correct status
    // Root cause: commit f0cc2e064 (Oct 1, 2026) in AsyncRestExecutor.java line 125
    //   uses BytesRestResponse(status, e.getMessage()) instead of reportError() → loses JSON
    assertEquals(400, exception.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(exception.getResponse());
    assertTrue(
        "Error message should mention JSON_EXTRACT",
        body.contains("JSON_EXTRACT") || body.contains("not supported"));

    // TODO: Fix AsyncRestExecutor to use ErrorMessageFactory, then assert JSON format:
    // JSONObject error = new JSONObject(body);
    // assertTrue(error.has("error") && error.has("status"));
  }
}
