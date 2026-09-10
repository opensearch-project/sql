/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Security tests for the PPL → Query Insights reporting path on an FGAC (security-enabled) cluster.
 *
 * <p>Reporting a completed PPL query is an internal {@code cluster:admin/...} action the end user
 * never invokes; the send is wrapped in {@code stashContext()} so it runs user-less rather than
 * being authorized against the querying user (see {@code QueryInsightsReporter}). These tests pin
 * that contract: a low-privilege user who holds only PPL + index-read permissions — and explicitly
 * NOT the report action's {@code cluster:admin} permission — can still run PPL queries, including
 * the multi-scan (join) path that stamps the parent marker for child sub-query tagging.
 *
 * <p>Read-side visibility of the recorded user identity is governed by the Query Insights plugin's
 * own RBAC filter ({@code search.insights.top_queries.filter_by_mode}); that plugin is not
 * installed in the SQL integ-test cluster, so it is covered by Query Insights' own tests rather
 * than here.
 */
public class QueryInsightsReportingSecurityIT extends SecurityTestBase {

  private static final String PPL_USER = "qi_ppl_user";
  private static final String PPL_ROLE = "qi_ppl_role";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    super.init();
    if (!initialized) {
      // PPL + index read/mapping/settings, but deliberately NO report action permission
      // (cluster:admin/opensearch/query_insights/report_query_bytes) and no all_access.
      createRoleWithPermissions(
          PPL_ROLE,
          TEST_INDEX_BANK,
          new String[] {"cluster:admin/opensearch/ppl"},
          new String[] {
            "indices:data/read/search*",
            "indices:admin/mappings/get",
            "indices:monitor/settings/get",
            "indices:data/read/point_in_time/create",
            "indices:data/read/point_in_time/delete"
          });
      createUser(PPL_USER, PPL_ROLE);
      loadIndex(Index.BANK);
      enableCalcite();
      allowCalciteFallback();
      initialized = true;
    }
  }

  @Test
  public void lowPrivilegeUserCanRunPplQueryWithoutReportPermission() throws IOException {
    // The report send is stashed to Origin.LOCAL, so lacking the report cluster:admin permission
    // must not cause the user's own query to fail.
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | where age > 30 | fields firstname", TEST_INDEX_BANK),
            PPL_USER);
    verifyColumn(result, columnName("firstname"));
  }

  @Test
  public void lowPrivilegeUserCanRunStatsQueryWithoutReportPermission() throws IOException {
    JSONObject result =
        executeQueryAsUser(
            String.format("source=%s | stats count() by gender", TEST_INDEX_BANK), PPL_USER);
    verifyColumn(result, columnName("gender"), columnName("count()"));
  }
}
