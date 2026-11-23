/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.opensearch.sql.util.Capability.PAGINATION_CURSOR;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Verifies that the legacy engine answers with the status OpenSearch reported when OpenSearch
 * rejects the point in time a query needs. The HTTP status and the status in the body must both be
 * 429, so the client knows it can retry.
 */
@RequiresCapability(PAGINATION_CURSOR)
public class PointInTimeRejectionIT extends SQLIntegTestCase {

  private static final String MAX_OPEN_PIT_CONTEXT = "search.max_open_pit_context";

  private static final String JOIN_QUERY =
      String.format(
          Locale.ROOT,
          "SELECT c.gender, h.hname FROM %1$s c JOIN %1$s h ON h.hname = c.house",
          TEST_INDEX_GAME_OF_THRONES);

  @Override
  protected void init() throws Exception {
    loadIndex(Index.GAME_OF_THRONES);
  }

  @After
  public void resetMaxOpenPitContext() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, MAX_OPEN_PIT_CONTEXT, null));
  }

  @Test
  public void testJoinWithJdbcFormatReturns429() throws IOException {
    rejectPointInTime();
    assertRejected(sqlRequest(JOIN_QUERY, "jdbc", null));
  }

  @Test
  public void testJoinWithCsvFormatReturns429() throws IOException {
    rejectPointInTime();
    assertRejected(sqlRequest(JOIN_QUERY, "csv", null));
  }

  @Test
  public void testFetchSizeWithJdbcFormatReturns429() throws IOException {
    rejectPointInTime();
    String query =
        String.format(
            Locale.ROOT, "SELECT name.firstname FROM %s LIMIT 5", TEST_INDEX_GAME_OF_THRONES);
    assertRejected(sqlRequest(query, "jdbc", 1));
  }

  private void rejectPointInTime() throws IOException {
    updateClusterSettings(new ClusterSetting(TRANSIENT, MAX_OPEN_PIT_CONTEXT, "0"));
  }

  private Request sqlRequest(String query, String format, Integer fetchSize) {
    Request request = getSqlRequest("{}", false, format);
    JSONObject body = new JSONObject().put("query", query);
    if (fetchSize != null) {
      body.put("fetch_size", fetchSize);
    }
    request.setJsonEntity(body.toString());
    return request;
  }

  private void assertRejected(Request request) throws IOException {
    ResponseException e = expectThrows(ResponseException.class, () -> executeRequest(request));
    Response response = e.getResponse();
    String body = TestUtils.getResponseBody(response);
    assertThat(body, response.getStatusLine().getStatusCode(), equalTo(429));
    assertThat(body, new JSONObject(body).getInt("status"), equalTo(429));
  }
}
