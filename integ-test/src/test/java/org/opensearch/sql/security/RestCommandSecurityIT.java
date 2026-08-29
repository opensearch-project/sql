/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Integration tests that the rest command is subject to the security plugin fine grained access
 * control. The command dispatches a standard transport action under the caller identity, so the
 * security ActionFilter authorizes it by action name. This version ships only {@code
 * /_cluster/health}, which requires the {@code cluster:monitor/health} privilege: a caller holding
 * cluster monitor can run it, a caller without it is denied. The command therefore grants no access
 * beyond calling the endpoint natively.
 */
public class RestCommandSecurityIT extends SecurityTestBase {

  private static final String MONITOR_USER = "rest_monitor_user";
  private static final String MONITOR_ROLE = "rest_monitor_role";

  private static final String NO_MONITOR_USER = "rest_no_monitor_user";
  private static final String NO_MONITOR_ROLE = "rest_no_monitor_role";

  @Override
  protected void init() throws Exception {
    org.junit.Assume.assumeTrue(
        "opensearch-security plugin not installed on test cluster; skipping FGAC tests",
        org.opensearch.sql.util.ClusterPlugins.isPluginInstalled(
            client(), org.opensearch.sql.util.ClusterPlugins.SECURITY_PLUGIN));
    super.init();
    setupRolesAndUsers();
    enableCalcite();
    // rest is Calcite only, so a V2 fallback would replace the security denial with an unsupported
    // command error. Disable fallback so the denial reason surfaces to the caller.
    disallowCalciteFallback();
  }

  private void setupRolesAndUsers() throws IOException {
    createRoleWithPermissions(
        MONITOR_ROLE,
        "*",
        new String[] {"cluster:admin/opensearch/ppl", "cluster:monitor/health"},
        new String[] {});
    createUser(MONITOR_USER, MONITOR_ROLE);

    createRoleWithPermissions(
        NO_MONITOR_ROLE, "*", new String[] {"cluster:admin/opensearch/ppl"}, new String[] {});
    createUser(NO_MONITOR_USER, NO_MONITOR_ROLE);
  }

  @Test
  public void monitorUserCanRunClusterHealth() throws IOException {
    JSONObject result =
        executeQueryAsUser("| rest '/_cluster/health' | fields response", MONITOR_USER);
    verifyColumn(result, columnName("response"));
  }

  @Test
  public void userWithoutClusterMonitorCannotRunClusterHealth() throws IOException {
    assertDenied(
        "| rest '/_cluster/health' | fields response", NO_MONITOR_USER, "cluster:monitor/health");
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
}
