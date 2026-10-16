/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.QueryInsightsRecords.awaitRecord;
import static org.opensearch.sql.util.QueryInsightsRecords.pplRecordFor;
import static org.opensearch.sql.util.QueryInsightsRecords.stringsOf;
import static org.opensearch.sql.util.QueryInsightsRecords.topQueries;

import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.util.ClusterPlugins;
import org.opensearch.sql.util.QueryInsightsRecords;

/**
 * Query Insights recording on a security-enabled cluster: whose identity a PPL record carries, and
 * who can read it back.
 *
 * <p>Runs only in {@code :integ-test:integTestWithQueryInsightsAndSecurity}, whose cluster installs
 * both Query Insights and the security plugin. {@link QueryInsightsReportingSecurityIT} covers a
 * secured cluster without Query Insights.
 *
 * <p>Each user queries an index named after them, so a record is identified by its index.
 */
public class QueryInsightsRecordingSecurityIT extends SecurityTestBase {

  /**
   * Holds PPL and read access to its index but no Query Insights permission at all, including the
   * {@code cluster:admin} report action the plugin sends when the query finishes.
   */
  private static final String PPL_ONLY_USER = "qi_sec_carol";

  /** Can also read Top N; with filter_by_mode=username each should see only their own queries. */
  private static final String ALICE = "qi_sec_alice";

  private static final String BOB = "qi_sec_bob";

  private static final String TOP_QUERIES_ACTION = "cluster:admin/opensearch/insights/top_queries";
  private static final String FILTER_BY_MODE = "search.insights.top_queries.filter_by_mode";

  private static final long CLASS_START_MILLIS = System.currentTimeMillis();

  private static boolean usersCreated = false;

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    ClusterPlugins.requirePluginOrAssume(
        client(), ClusterPlugins.SECURITY_PLUGIN, "security plugin is not installed");
    QueryInsightsRecords.assertSupportsPpl(client());
    QueryInsightsRecords.enableRecording(client());
    if (!usersCreated) {
      for (String user : List.of(PPL_ONLY_USER, ALICE, BOB)) {
        QueryInsightsRecords.indexWithDocs(client(), user);
      }
      createRoleWithIndexAccess(roleOf(PPL_ONLY_USER), "qi_sec_*");
      createUser(PPL_ONLY_USER, roleOf(PPL_ONLY_USER));
      for (String user : List.of(ALICE, BOB)) {
        createRoleWithPermissions(
            roleOf(user),
            "qi_sec_*",
            new String[] {"cluster:admin/opensearch/ppl", TOP_QUERIES_ACTION},
            PPL_INDEX_PERMISSIONS);
        createUser(user, roleOf(user));
      }
      usersCreated = true;
    }
  }

  @Test
  public void recordCarriesTheIdentityOfAUserWithoutAnyInsightsPermission() throws Exception {
    executeQueryAsUser("source=" + PPL_ONLY_USER + " | where v > 1 | fields v", PPL_ONLY_USER);

    // Read as admin. The record exists although the user holds no Query Insights permission, so
    // the report is not authorized against the user who ran the query.
    JSONObject record = awaitRecordFor(PPL_ONLY_USER);
    assertEquals(PPL_ONLY_USER, record.optString("username"));
    JSONArray roles = record.optJSONArray("user_roles");
    assertTrue(
        "record must carry the user's roles, was " + roles,
        stringsOf(roles).contains(roleOf(PPL_ONLY_USER)));
  }

  @Test
  public void userWithoutInsightsPermissionCannotReadTopN() throws Exception {
    ResponseException e =
        expectThrows(
            ResponseException.class,
            () -> topQueries(client(), asUser(PPL_ONLY_USER), CLASS_START_MILLIS, r -> true));
    assertEquals(403, e.getResponse().getStatusLine().getStatusCode());
  }

  @Test
  public void withFilterByUsernameEachUserSeesOnlyTheirOwnQueries() throws Exception {
    QueryInsightsRecords.putPersistentSettings(client(), Map.of(FILTER_BY_MODE, "username"));
    try {
      executeQueryAsUser("source=" + ALICE + " | where v > 1 | fields v", ALICE);
      executeQueryAsUser("source=" + BOB + " | where v > 1 | fields v", BOB);
      // Admin sees everything, so both records must exist before checking what each user sees.
      awaitRecordFor(ALICE);
      awaitRecordFor(BOB);

      assertSeesOnlyOwnQueries(ALICE, BOB);
      assertSeesOnlyOwnQueries(BOB, ALICE);
    } finally {
      QueryInsightsRecords.putPersistentSettings(client(), Map.of(FILTER_BY_MODE, "none"));
    }
  }

  // ---------------------------------------------------------------------------------------------

  private void assertSeesOnlyOwnQueries(String user, String other) throws Exception {
    List<JSONObject> visible =
        topQueries(client(), asUser(user), CLASS_START_MILLIS, QueryInsightsRecords::isPpl);
    assertTrue(
        user + " sees none of their own queries", visible.stream().anyMatch(pplRecordFor(user)));
    assertTrue(
        user + " sees " + other + "'s queries: " + visible,
        visible.stream().noneMatch(pplRecordFor(other)));
    for (JSONObject record : visible) {
      assertEquals(
          user + " sees another user's record: " + record, user, record.optString("username"));
    }
  }

  private static JSONObject awaitRecordFor(String user) throws Exception {
    return awaitRecord(
        client(),
        RequestOptions.DEFAULT,
        CLASS_START_MILLIS,
        pplRecordFor(user),
        "PPL record for " + user);
  }

  private RequestOptions asUser(String user) {
    return RequestOptions.DEFAULT.toBuilder()
        .addHeader("Authorization", createBasicAuthHeader(user, STRONG_PASSWORD))
        .build();
  }

  private static String roleOf(String user) {
    return user + "_role";
  }
}
