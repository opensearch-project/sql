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
 * Runs PPL as a low-privilege user on the FGAC (security-enabled) cluster with the Query Insights
 * reporting code present.
 *
 * <p>Query Insights is not installed in this cluster, so the reporting gate ({@code
 * search.insights.top_queries.ppl.enabled}) is unregistered and the record is never sent. What
 * these tests do pin is the non-interference contract on a secured cluster: a user holding only PPL
 * + index-read permissions, and explicitly not the report action's {@code cluster:admin}
 * permission, can run PPL queries unchanged.
 *
 * <p>The send itself is wrapped in {@code stashContext()} so it runs user-less rather than being
 * authorized against the querying user (see {@code QueryInsightsReporter}); proving that holds for
 * a low-privilege user needs Query Insights co-installed and belongs with that plugin's tests, as
 * does read-side visibility of the recorded identity ({@code
 * search.insights.top_queries.filter_by_mode}).
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
    // Lacking the report action's cluster:admin permission must not affect the user's own query.
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
