/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration tests that verify the rest command is subject to the security plugin fine grained
 * access control. The command dispatches standard transport actions under the caller identity, so
 * the security ActionFilter authorizes each one by action name. A caller without the required
 * cluster monitor privilege is denied the cat and cluster endpoints, a caller holding the privilege
 * can run them, and the resolve index endpoint requires the resolve index privilege because the
 * command resolves all indices.
 */
public class RestCommandSecurityIT extends SecurityTestBase {

  private static final String ALPHA_INDEX = "rest_sec_alpha";
  private static final String BETA_INDEX = "rest_sec_beta";

  private static final String MONITOR_USER = "rest_monitor_user";
  private static final String MONITOR_ROLE = "rest_monitor_role";

  private static final String NO_MONITOR_USER = "rest_no_monitor_user";
  private static final String NO_MONITOR_ROLE = "rest_no_monitor_role";

  @Override
  protected void init() throws Exception {
    super.init();
    setupRolesUsersAndIndices();
    enableCalcite();
    // rest is Calcite only, so a V2 fallback would replace the security denial with an unsupported
    // command error. Disable fallback so the denial reason surfaces to the caller.
    disallowCalciteFallback();
  }

  private void setupRolesUsersAndIndices() throws IOException {
    createIndexIfAbsent(ALPHA_INDEX);
    createIndexIfAbsent(BETA_INDEX);

    createRoleWithPermissions(
        MONITOR_ROLE,
        "*",
        new String[] {
          "cluster:admin/opensearch/ppl",
          "cluster:monitor/health",
          "cluster:monitor/state",
          "cluster:monitor/nodes/stats",
          "cluster:monitor/nodes/info"
        },
        new String[] {"indices:admin/resolve/index"});
    createUser(MONITOR_USER, MONITOR_ROLE);

    createRoleWithPermissions(
        NO_MONITOR_ROLE,
        ALPHA_INDEX,
        new String[] {"cluster:admin/opensearch/ppl"},
        new String[] {"indices:data/read/search*"});
    createUser(NO_MONITOR_USER, NO_MONITOR_ROLE);
  }

  @Test
  public void monitorUserCanRunCatNodes() throws IOException {
    JSONObject result = executeQueryAsUser("| rest '/_cat/nodes' | fields name", MONITOR_USER);
    verifyColumn(result, columnName("name"));
  }

  @Test
  public void monitorUserCanResolveIndex() throws IOException {
    JSONObject result =
        executeQueryAsUser("| rest '/_resolve/index' | fields name, type", MONITOR_USER);
    Set<String> names = resolvedNames(result);
    assertTrue("resolve should list authorized indices: " + names, names.contains(ALPHA_INDEX));
    assertTrue("resolve should list authorized indices: " + names, names.contains(BETA_INDEX));
  }

  @Test
  public void userWithoutClusterMonitorCannotRunCatNodes() throws IOException {
    assertDenied(
        "| rest '/_cat/nodes' | fields name", NO_MONITOR_USER, "cluster:monitor/nodes/stats");
  }

  @Test
  public void userWithoutClusterMonitorCannotRunClusterState() throws IOException {
    assertDenied(
        "| rest '/_cluster/state' | fields cluster_name", NO_MONITOR_USER, "cluster:monitor/state");
  }

  @Test
  public void userWithoutResolvePrivilegeCannotResolveIndex() throws IOException {
    assertDenied(
        "| rest '/_resolve/index' | fields name, type",
        NO_MONITOR_USER,
        "indices:admin/resolve/index");
  }

  /**
   * Asserts the query is rejected for a caller lacking the privilege. A denied transport action on
   * the Calcite only rest path surfaces as a client or server error whose body carries the security
   * denial reason, so this checks the denial signal rather than a fixed status code.
   */
  private void assertDenied(String query, String user, String deniedAction) throws IOException {
    try {
      executeQueryAsUser(query, user);
      fail("Expected a permission denial for user without privilege: " + user);
    } catch (ResponseException e) {
      int status = e.getResponse().getStatusLine().getStatusCode();
      String body = TestUtils.getResponseBody(e.getResponse(), false);
      assertTrue("Expected an error status, got " + status, status >= 400);
      assertTrue(
          "Response should indicate a permission denial. Status " + status + ", body: " + body,
          body.contains("no permissions")
              || body.contains("Forbidden")
              || body.contains("security_exception")
              || body.contains(deniedAction));
    }
  }

  private void createIndexIfAbsent(String name) throws IOException {
    Request request = new Request("PUT", "/" + name);
    request.setJsonEntity(
        "{ \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 } }");
    try {
      client().performRequest(request);
    } catch (ResponseException e) {
      String body = TestUtils.getResponseBody(e.getResponse(), false);
      if (!body.contains("resource_already_exists_exception")) {
        throw e;
      }
    }
  }

  private Set<String> resolvedNames(JSONObject result) {
    Set<String> names = new HashSet<>();
    JSONArray datarows = result.getJSONArray("datarows");
    for (int i = 0; i < datarows.length(); i++) {
      names.add(datarows.getJSONArray(i).getString(0));
    }
    return names;
  }
}
