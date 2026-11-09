/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.util.Capability.TIME_BOUNDS_PRUNING;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.util.ClusterPlugins;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Request-level time bounds with the security plugin installed: the bounds must survive the
 * transport-to-worker handoff, which drops Log4j {@code ThreadContext}, and the probes must be
 * permitted. Both failures are silent, so this observes the resolved schema.
 */
@RequiresCapability(TIME_BOUNDS_PRUNING)
public class TimeBoundsPruningSecurityIT extends SecurityTestBase {

  private static final String OLD_INDEX = "prune_sec_000001";
  private static final String NEW_INDEX = "prune_sec_000002";
  private static final String PATTERN = "prune_sec_*";

  private static final String USER = "prune_sec_user";
  private static final String ROLE = "prune_sec_role";

  /** Covers only {@link #NEW_INDEX}'s documents. */
  private static final String FROM = "2026-09-10 00:00:00.000";

  private static final String TO = "2026-09-10 23:59:59.999";

  private static final String IN_RANGE =
      "source=" + PATTERN + " | where `ts` >= '" + FROM + "' AND `ts` <= '" + TO + "'";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
    ClusterPlugins.requirePluginOrAssume(
        client(),
        ClusterPlugins.SECURITY_PLUGIN,
        "opensearch-security plugin not installed on test cluster; skipping FGAC tests");
    super.init();
    enableCalcite();
    if (!initialized) {
      createRoleWithIndexAccess(ROLE, PATTERN);
      createUser(USER, ROLE);
      createRolloverIndices();
      initialized = true;
    }
    setPruning(true);
  }

  @After
  public void resetPruning() throws IOException {
    resetPruningToDefault();
  }

  /** A rollover that dropped a field: {@code legacy_only} exists only in the older index. */
  private void createRolloverIndices() throws IOException {
    if (!isIndexExist(client(), OLD_INDEX)) {
      createIndexByRestClient(
          client(),
          OLD_INDEX,
          "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"},"
              + "\"legacy_only\":{\"type\":\"keyword\"}}}}");
      Request bulk = new Request("POST", "/" + OLD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"ts\":\"2026-06-01T10:00:00Z\",\"legacy_only\":\"pre-rollover\"}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), NEW_INDEX)) {
      createIndexByRestClient(
          client(), NEW_INDEX, "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"}}}}");
      Request bulk = new Request("POST", "/" + NEW_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"ts\":\"2026-09-10T10:00:00Z\"}\n"
              + "{\"index\":{}}\n{\"ts\":\"2026-09-10T11:00:00Z\"}\n");
      performRequest(client(), bulk);
    }
  }

  /** The merge saw only the in-range index, so the other's field is gone. */
  @Test
  public void boundsSurviveSecurityHandoffAndNarrowTheResolvedSchema() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () ->
                executeQueryAsUserWithBounds(
                    IN_RANGE + " | fields legacy_only", USER, "ts", FROM, TO));

    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    assertTrue(
        "legacy_only lives only in the out-of-range index, so pruning must remove it: "
            + e.getMessage(),
        e.getMessage().contains("Field [legacy_only] not found."));
  }

  /** Without bounds the merge still sees the older index, so the field resolves. */
  @Test
  public void sameQueryWithoutBoundsStillResolvesTheWholePattern() throws IOException {
    verifyDataRows(
        executeQueryAsUser(IN_RANGE + " | fields legacy_only | head 1", USER), rows((Object) null));
  }

  /** The bounds must do nothing unless the cluster opted in. */
  @Test
  public void boundsAreIgnoredWhenPruningIsDisabled() throws IOException {
    setPruning(false);

    verifyDataRows(
        executeQueryAsUserWithBounds(
            IN_RANGE + " | fields legacy_only | head 1", USER, "ts", FROM, TO),
        rows((Object) null));
  }

  /** Pruning decides which indices are read, never which rows come back. */
  @Test
  public void boundsDoNotChangeTheRowsReturned() throws IOException {
    verifyDataRows(executeQueryAsUser(IN_RANGE + " | stats count()", USER), rows(2));
    verifyDataRows(
        executeQueryAsUserWithBounds(IN_RANGE + " | stats count()", USER, "ts", FROM, TO), rows(2));
  }

  /** A range spanning both indices must leave the expression alone rather than drop either. */
  @Test
  public void boundsCoveringEveryIndexPruneNothing() throws IOException {
    String from = "2026-01-01 00:00:00.000";
    String to = "2026-12-31 23:59:59.999";
    String wide =
        "source="
            + PATTERN
            + " | where `ts` >= '"
            + from
            + "' AND `ts` <= '"
            + to
            + "'"
            + " | stats count()";

    verifyDataRows(executeQueryAsUserWithBounds(wide, USER, "ts", from, to), rows(3));
  }

  /** Like {@link #executeQueryAsUser}, but also sends the per-request time bounds. */
  private JSONObject executeQueryAsUserWithBounds(
      String query, String username, String timeField, String start, String end)
      throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{ \"query\": \"%s\", \"time_field\": \"%s\", \"start_time\": \"%s\","
                + " \"end_time\": \"%s\" }",
            query,
            timeField,
            start,
            end));
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    options.addHeader("Authorization", createBasicAuthHeader(username, STRONG_PASSWORD));
    request.setOptions(options);
    Response response = client().performRequest(request);
    assertEquals(200, response.getStatusLine().getStatusCode());
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  private void setPruning(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(),
            Boolean.toString(enabled)));
  }

  /** Clears rather than pins false: on by default since #5759, so a false would leak. */
  private void resetPruningToDefault() throws IOException {
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), null));
  }
}
