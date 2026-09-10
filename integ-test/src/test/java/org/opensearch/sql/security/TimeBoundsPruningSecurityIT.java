/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

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

/**
 * Runs the request-level time bounds with the security plugin installed.
 *
 * <p>The hint rides from the transport thread to the worker on the plan, not in Log4j {@code
 * ThreadContext}, because the security plugin's transport interceptor drops {@code ThreadContext}
 * on that handoff -- the bug #5739 and #5758 each had to fix for a different per-request signal. A
 * hint lost that way is silent: the query still returns the right rows, just over every index the
 * wildcard matches, so nothing but a test that observes the resolved schema can tell.
 *
 * <p>These cases use a mapping that conflicts only across the range boundary, so whether the hint
 * arrived is visible in the response rather than only in timings.
 */
public class TimeBoundsPruningSecurityIT extends SecurityTestBase {

  private static final String OLD_INDEX = "prune_sec_000001";
  private static final String NEW_INDEX = "prune_sec_000002";
  private static final String PATTERN = "prune_sec_*";

  private static final String USER = "prune_sec_user";
  private static final String ROLE = "prune_sec_role";

  /** Covers only {@link #NEW_INDEX}'s documents. */
  private static final String FROM = "2026-09-10 00:00:00.000";

  private static final String TO = "2026-09-10 23:59:59.999";

  private static final String QUERY =
      "source="
          + PATTERN
          + " | where `ts` >= '"
          + FROM
          + "' AND `ts` <= '"
          + TO
          + "' | chart count() over ts by attributes.cluster";

  private boolean initialized = false;

  @Override
  protected void init() throws Exception {
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
    setPruning(false);
  }

  /**
   * A rollover that changed the shape of a field: {@code attributes.cluster} was an object holding
   * {@code name}, and is a plain keyword after the roll. Merged, the object wins and charting by it
   * fails; pruned to the newer index alone, it is the keyword the query can pivot on.
   */
  private void createRolloverIndices() throws IOException {
    if (!isIndexExist(client(), OLD_INDEX)) {
      createIndexByRestClient(
          client(),
          OLD_INDEX,
          "{\"mappings\":{\"properties\":{"
              + "\"ts\":{\"type\":\"date\"},"
              + "\"attributes\":{\"properties\":{\"cluster\":{\"properties\":{"
              + "\"name\":{\"type\":\"keyword\"}}}}}}}}");
      Request bulk = new Request("POST", "/" + OLD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n"
              + "{\"ts\":\"2026-06-01T10:00:00Z\",\"attributes\":{\"cluster\":{\"name\":\"alpha\"}}}\n");
      performRequest(client(), bulk);
    }
    if (!isIndexExist(client(), NEW_INDEX)) {
      createIndexByRestClient(
          client(),
          NEW_INDEX,
          "{\"mappings\":{\"properties\":{"
              + "\"ts\":{\"type\":\"date\"},"
              + "\"attributes\":{\"properties\":{\"cluster\":{\"type\":\"keyword\"}}}}}}");
      Request bulk = new Request("POST", "/" + NEW_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n"
              + "{\"ts\":\"2026-09-10T10:00:00Z\",\"attributes\":{\"cluster\":\"gamma\"}}\n"
              + "{\"index\":{}}\n"
              + "{\"ts\":\"2026-09-10T11:00:00Z\",\"attributes\":{\"cluster\":\"delta\"}}\n");
      performRequest(client(), bulk);
    }
  }

  @Test
  public void hintSurvivesSecurityHandoffAndNarrowsTheResolvedSchema() throws IOException {
    JSONObject result = executeQueryAsUserWithBounds(QUERY, USER, "ts", FROM, TO);

    verifyDataRows(
        result, rows("2026-09-10 10:00:00", "gamma", 1), rows("2026-09-10 11:00:00", "delta", 1));
  }

  /** Without the hint the merge still sees the older index, so this is the failure it prevents. */
  @Test
  public void sameQueryWithoutTheHintStillSeesTheConflictingMapping() throws IOException {
    ResponseException e =
        assertThrows(ResponseException.class, () -> executeQueryAsUser(QUERY, USER));

    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
    assertTrue(
        "should fail on the merged object mapping the hint would have pruned away",
        e.getMessage().contains("Cannot chart by [attributes.cluster] because it is an object."));
  }

  /** The hint must do nothing unless the cluster opted in. */
  @Test
  public void hintIsIgnoredWhenPruningIsDisabled() throws IOException {
    setPruning(false);

    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeQueryAsUserWithBounds(QUERY, USER, "ts", FROM, TO));

    assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
  }

  /** A range spanning both indices must leave the expression alone rather than drop either. */
  @Test
  public void aHintCoveringEveryIndexPrunesNothing() throws IOException {
    String wide =
        "source="
            + PATTERN
            + " | where `ts` >= '2026-01-01 00:00:00.000' AND `ts` <= '2026-12-31 23:59:59.999'"
            + " | stats count()";

    JSONObject result =
        executeQueryAsUserWithBounds(
            wide, USER, "ts", "2026-01-01 00:00:00.000", "2026-12-31 23:59:59.999");

    verifyDataRows(result, rows(3));
  }

  /** Like {@link #executeQueryAsUser}, but also sends the per-request time bounds. */
  private JSONObject executeQueryAsUserWithBounds(
      String query, String username, String field, String from, String to) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{ \"query\": \"%s\", \"time_field\": \"%s\", \"start_time\": \"%s\","
                + " \"end_time\": \"%s\" }",
            query,
            field,
            from,
            to));
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
}
