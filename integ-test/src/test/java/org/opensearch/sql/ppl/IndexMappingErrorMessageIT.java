/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.json.JSONObject;
import org.opensearch.client.ResponseException;

/**
 * Verifies that the improved empty-mapping error path is surfaced correctly end-to-end.
 *
 * <p>We exercise two shapes:
 *
 * <ol>
 *   <li>A wildcard pattern that matches no indices — OpenSearch returns an empty mapping response;
 *       the plugin must return HTTP 404, error.code=INDEX_NOT_FOUND, and details that name the
 *       pattern and advise the user to check mapping compatibility.
 *   <li>A plain exact index name that does not exist — also 404/INDEX_NOT_FOUND but the details
 *       should NOT contain the "compatible mapping" hint (that hint is for wildcard patterns only).
 * </ol>
 *
 * <p>Note: the third scenario from the original bug report — a wildcard that matches multiple
 * existing indices with <em>incompatible</em> mappings — cannot be reliably reproduced in a unit
 * test cluster because OpenSearch returns each index's mapping individually (not an empty combined
 * response) for simple type conflicts. That scenario is covered by the {@link
 * org.opensearch.sql.opensearch.client.OpenSearchClient#emptyMappingException} unit tests and the
 * production logic path is identical to the "no matching indices" case tested here.
 */
public class IndexMappingErrorMessageIT extends PPLIntegTestCase {

  // Deliberately unique names so they never collide with real test indices.
  private static final String WILDCARD_PATTERN = "errmsg_it_no_such_idx_*";
  private static final String EXACT_NAME = "errmsg_it_no_such_exact";

  /**
   * Querying a wildcard pattern that matches no indices triggers the empty-mapping path. The plugin
   * must return HTTP 404 with INDEX_NOT_FOUND and a "compatible mapping" hint.
   */
  public void test_wildcard_pattern_with_no_matching_indices_returns_404_with_hint()
      throws Exception {
    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () -> executeQueryToString("source=" + WILDCARD_PATTERN + " | fields age"));

    String body = EntityUtils.toString(ex.getResponse().getEntity());
    JSONObject json = new JSONObject(body);

    assertEquals(404, json.getInt("status"));
    JSONObject error = json.getJSONObject("error");
    assertEquals("INDEX_NOT_FOUND", error.getString("code"));
    String details = error.getString("details");
    assertTrue("details should contain the pattern", details.contains("errmsg_it_no_such_idx_"));
    assertTrue(
        "details should hint at incompatible mappings", details.contains("compatible mapping"));
  }

  /**
   * Querying a plain non-wildcard index name that does not exist returns 404/INDEX_NOT_FOUND. The
   * details should NOT contain the "compatible mapping" hint.
   */
  public void test_exact_nonexistent_index_returns_404_without_mapping_hint() throws Exception {
    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () -> executeQueryToString("source=" + EXACT_NAME + " | fields age"));

    String body = EntityUtils.toString(ex.getResponse().getEntity());
    JSONObject json = new JSONObject(body);

    assertEquals(404, json.getInt("status"));
    JSONObject error = json.getJSONObject("error");
    assertEquals("INDEX_NOT_FOUND", error.getString("code"));
    assertFalse(
        "details should NOT contain compatible mapping hint for exact index names",
        error.getString("details").contains("compatible mapping"));
  }
}
