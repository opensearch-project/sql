/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import java.util.Base64;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Regression test for SQL cursor pagination under Fine-Grained Access Control (FGAC).
 *
 * <p>Exercises the legacy V1 cursor path (triggered by {@code SELECT ... LIMIT n} with {@code
 * fetch_size}). Before the fix, page 2 would return 403 because the continuation SearchRequest was
 * created with no indices, which Security resolves to a wildcard and denies under FGAC.
 */
public class SQLCursorPermissionsIT extends SQLIntegTestCase {

  private static final String ACCOUNT_USER = "account_cursor_user";
  private static final String ACCOUNT_ROLE = "account_cursor_role";
  private static final String STRONG_PASSWORD = "StrongPassword123!";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    loadIndex(Index.ACCOUNT);
    createSecurityRolesAndUsers();
  }

  private void createSecurityRolesAndUsers() throws IOException {
    if (initialized) {
      return;
    }
    createRole(ACCOUNT_ROLE, TEST_INDEX_ACCOUNT);
    createUser(ACCOUNT_USER, ACCOUNT_ROLE);
    initialized = true;
  }

  private void createRole(String roleName, String indexPattern) throws IOException {
    Request request = new Request("PUT", "/_plugins/_security/api/roles/" + roleName);
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "cluster_permissions": [
                "cluster:admin/opensearch/ppl",
                "cluster:admin/opensearch/sql"
              ],
              "index_permissions": [{
                "index_patterns": [
                  "%s"
                ],
                "allowed_actions": [
                  "indices:data/read/search*",
                  "indices:admin/mappings/get",
                  "indices:admin/mappings/fields/get*",
                  "indices:monitor/settings/get",
                  "indices:data/read/point_in_time/create",
                  "indices:data/read/point_in_time/delete"
                ]
              }]
            }
            """,
            indexPattern));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    request.setOptions(opts);

    Response response = client().performRequest(request);
    int status = response.getStatusLine().getStatusCode();
    assertTrue(status == 200 || status == 201);
  }

  private void createUser(String username, String roleName) throws IOException {
    Request userRequest = new Request("PUT", "/_plugins/_security/api/internalusers/" + username);
    userRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "password": "%s",
              "backend_roles": [],
              "attributes": {}
            }
            """,
            STRONG_PASSWORD));
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    userRequest.setOptions(opts);

    Response userResponse = client().performRequest(userRequest);
    int userStatus = userResponse.getStatusLine().getStatusCode();
    assertTrue(userStatus == 200 || userStatus == 201);

    Request mappingRequest = new Request("PUT", "/_plugins/_security/api/rolesmapping/" + roleName);
    mappingRequest.setJsonEntity(
        String.format(
            Locale.ROOT,
            """
            {
              "backend_roles": [],
              "hosts": [],
              "users": ["%s"]
            }
            """,
            username));
    mappingRequest.setOptions(opts);

    Response mappingResponse = client().performRequest(mappingRequest);
    int mappingStatus = mappingResponse.getStatusLine().getStatusCode();
    assertTrue(mappingStatus == 200 || mappingStatus == 201);
  }

  private JSONObject executeSqlAsUser(String body, String username) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(body);
    RequestOptions.Builder opts = RequestOptions.DEFAULT.toBuilder();
    opts.addHeader("Content-Type", "application/json");
    opts.addHeader(
        "Authorization",
        "Basic "
            + Base64.getEncoder().encodeToString((username + ":" + STRONG_PASSWORD).getBytes()));
    request.setOptions(opts);

    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(TestUtils.getResponseBody(response, true));
  }

  @Test
  public void simpleSelectUnderFgacSucceeds() throws IOException {
    JSONObject result =
        executeSqlAsUser(
            String.format(
                Locale.ROOT,
                "{\"query\": \"SELECT firstname FROM %s LIMIT 1\"}",
                TEST_INDEX_ACCOUNT),
            ACCOUNT_USER);
    assertTrue(result.has("datarows"));
  }

  /**
   * Regression for SQL cursor pagination under FGAC. Triggers the V1 cursor path (LIMIT with
   * fetch_size) and advances through multiple continuation pages. Before the fix, page 2 returned
   * 403 because the continuation SearchRequest carried no indices, which Security resolves to a
   * wildcard and denies.
   */
  @Test
  public void cursorPaginationUnderFgacSucceedsAcrossPages() throws IOException {
    // LIMIT forces the V1 cursor path (V2's CanPaginateVisitor rejects LIMIT). The V1 path is
    // the one that constructs the continuation SearchRequest without indices, which Security
    // denies under FGAC before this fix.
    JSONObject firstPage =
        executeSqlAsUser(
            String.format(
                Locale.ROOT,
                "{\"fetch_size\": 50, \"query\": \"SELECT age, balance FROM %s LIMIT 234\"}",
                TEST_INDEX_ACCOUNT),
            ACCOUNT_USER);
    assertTrue("first page must include a cursor; body=" + firstPage, firstPage.has("cursor"));
    String cursor = firstPage.getString("cursor");
    assertFalse("first page cursor must not be empty", cursor.isEmpty());
    assertTrue(
        "expected V1 cursor (prefix 'd:'), got: "
            + cursor.substring(0, Math.min(6, cursor.length())),
        cursor.startsWith("d:"));

    int pages = 1;
    while (!cursor.isEmpty()) {
      JSONObject next =
          executeSqlAsUser(
              String.format(Locale.ROOT, "{\"cursor\": \"%s\"}", cursor), ACCOUNT_USER);
      cursor = next.optString("cursor", "");
      pages++;
    }
    // 234 rows / 50 per page = 5 pages
    assertEquals("expected 5 V1 cursor pages under FGAC", 5, pages);
  }
}
