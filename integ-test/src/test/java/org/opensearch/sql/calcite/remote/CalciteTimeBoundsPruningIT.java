/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

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
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * The {@code start_time}/{@code end_time} request parameters.
 *
 * <p>Observed through the resolved schema, not timings: a field only the out-of-range index maps
 * stops resolving once that index is gone.
 */
public class CalciteTimeBoundsPruningIT extends PPLIntegTestCase {

  private static final String OLD_INDEX = "prune_range_000001";
  private static final String NEW_INDEX = "prune_range_000002";
  private static final String PATTERN = "prune_range_*";

  /** Covers only {@link #NEW_INDEX}'s documents. */
  private static final String FROM = "2026-09-10 00:00:00.000";

  private static final String TO = "2026-09-10 23:59:59.999";

  /** An instant inside {@link #FROM}..{@link #TO}. */
  private static final String IN_RANGE_INSTANT = "2026-09-10T10:30:00Z";

  private static final String IN_RANGE =
      "source=" + PATTERN + " | where `ts` >= '" + FROM + "' AND `ts` <= '" + TO + "'";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createRolloverIndices();
    setPruning(true);
  }

  @After
  public void resetPruning() throws IOException {
    resetPruningToDefault();
  }

  /** {@code legacy_only} exists only in the older index, so its resolution names what merged. */
  private void createRolloverIndices() throws IOException {
    if (!isIndexExist(client(), OLD_INDEX)) {
      createIndexByRestClient(
          client(),
          OLD_INDEX,
          "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"},"
              + "\"legacy_only\":{\"type\":\"keyword\"}}}}");
      Request bulk = new Request("POST", "/" + OLD_INDEX + "/_bulk?refresh=true");
      bulk.setJsonEntity(
          "{\"index\":{}}\n{\"ts\":\"2026-06-01T10:00:00Z\",\"legacy_only\":\"old\"}\n");
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

  @Test
  public void shouldResolveTheSchemaFromTheInRangeIndicesOnly() {
    ResponseException e =
        assertThrows(
            ResponseException.class,
            () -> executeWithBounds(IN_RANGE + " | fields legacy_only", "ts", FROM, TO));

    assertTrue(
        "legacy_only lives only in the out-of-range index, so it must be gone: " + e.getMessage(),
        e.getMessage().contains("Field [legacy_only] not found."));
  }

  @Test
  public void shouldResolveTheWholePatternWithoutBounds() throws IOException {
    verifyDataRows(executeQuery(IN_RANGE + " | fields legacy_only | head 1"), rows((Object) null));
  }

  /** Pruning must not change a single row. */
  @Test
  public void shouldReturnTheSameRowsWithAndWithoutBounds() throws IOException {
    verifyDataRows(executeQuery(IN_RANGE + " | stats count()"), rows(2));
    verifyDataRows(executeWithBounds(IN_RANGE + " | stats count()", "ts", FROM, TO), rows(2));
  }

  @Test
  public void shouldPruneNothingWhenTheRangeCoversEveryIndex() throws IOException {
    String from = "2026-01-01 00:00:00.000";
    String to = "2026-12-31 23:59:59.999";
    String query =
        "source="
            + PATTERN
            + " | where `ts` >= '"
            + from
            + "' AND `ts` <= '"
            + to
            + "'"
            + " | stats count()";

    verifyDataRows(executeWithBounds(query, "ts", from, to), rows(3));
  }

  @Test
  public void shouldIgnoreBoundsWhenPruningIsDisabled() throws IOException {
    setPruning(false);

    verifyDataRows(
        executeWithBounds(IN_RANGE + " | fields legacy_only | head 1", "ts", FROM, TO),
        rows((Object) null));
  }

  /** Unusable bounds are dropped, not an error. */
  @Test
  public void shouldIgnoreUnusableBounds() throws IOException {
    verifyDataRows(
        executeWithBounds(IN_RANGE + " | stats count()", "ts", "Invalid date", TO), rows(2));
    verifyDataRows(executeWithBounds(IN_RANGE + " | stats count()", "", FROM, TO), rows(2));
    // Inverted: a client mistake, not a range.
    verifyDataRows(executeWithBounds(IN_RANGE + " | stats count()", "ts", TO, FROM), rows(2));
  }

  /** A field no index maps must leave the expression alone. */
  @Test
  public void shouldIgnoreBoundsOnAnUnmappedField() throws IOException {
    verifyDataRows(
        executeWithBounds(IN_RANGE + " | stats count()", "no_such_field", FROM, TO), rows(2));
  }

  /**
   * Request-level scope: the bounds reach a join's other side too, even when it reads a wildcard
   * the outer query never names. #5766 review (@penghuo).
   */
  @Test
  public void shouldApplyTheBoundsToAJoinsOtherSide() throws IOException {
    String ref = "prune_range_join_ref";
    String refOld = ref + "-old";
    String refNew = ref + "-new";
    seedJoinRef(refOld, "2026-06-01T12:00:00Z");
    seedJoinRef(refNew, IN_RANGE_INSTANT);
    try {
      String query =
          "source="
              + NEW_INDEX
              + " | head 1 | eval k = 1 | join left=l right=r on l.k = r.k"
              + " [ source="
              + ref
              + "-* | stats count() as ref_rows by k ]"
              + " | fields ref_rows | head 1";

      // Both reference indices contribute.
      verifyDataRows(executeQuery(query), rows(2));
      // The out-of-window one is pruned from the join's own source, so only one does.
      verifyDataRows(executeWithBounds(query, "ts", FROM, TO), rows(1));
    } finally {
      client().performRequest(new Request("DELETE", "/" + refOld + "," + refNew));
    }
  }

  private void seedJoinRef(String index, String instant) throws IOException {
    if (!isIndexExist(client(), index)) {
      createIndexByRestClient(
          client(),
          index,
          "{\"mappings\":{\"properties\":{\"ts\":{\"type\":\"date\"},\"k\":{\"type\":\"integer\"}}}}");
      Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
      bulk.setJsonEntity("{\"index\":{}}\n{\"ts\":\"" + instant + "\",\"k\":1}\n");
      performRequest(client(), bulk);
    }
  }

  /** Request-level scope: the bounds reach a subsearch's source too. */
  @Test
  public void shouldApplyTheBoundsToASubsearchToo() throws IOException {
    String query =
        IN_RANGE
            + " | eval k = 1 | join left=l right=r on l.k = r.k"
            + " [ source="
            + PATTERN
            + " | where `ts` <= '2026-07-01 00:00:00.000' | eval k = 1"
            + " | stats count() as out_of_range_rows by k ]"
            + " | fields out_of_range_rows | head 1";

    // Without bounds the subsearch finds its row in the older index.
    verifyDataRows(executeQuery(query), rows(1));
    // With them the older index is not read, so the join drops the row.
    assertEquals(0, executeWithBounds(query, "ts", FROM, TO).getInt("total"));
  }

  /**
   * A document with no time value is in no window, so its index is pruned -- as {@code
   * index_pruning.yml} asserts for the filter path. Deliberate, and documented.
   */
  @Test
  public void shouldPruneAnIndexThatDoesNotMapTheTimeField() throws IOException {
    String noTimeField = PATTERN.replace("*", "notime");
    if (!isIndexExist(client(), noTimeField)) {
      createIndexByRestClient(
          client(),
          noTimeField,
          "{\"mappings\":{\"properties\":{\"other\":{\"type\":\"keyword\"}}}}");
      Request bulk = new Request("POST", "/" + noTimeField + "/_bulk?refresh=true");
      bulk.setJsonEntity("{\"index\":{}}\n{\"other\":\"keep-me\"}\n");
      performRequest(client(), bulk);
    }
    try {
      String query = "source=" + PATTERN + " | where isnotnull(other) | stats count() as total";
      // The row, and the only mapping of `other`, live in that index.
      verifyDataRows(executeQuery(query), rows(1));
      // Excluded, so `other` stops resolving too.
      ResponseException e =
          assertThrows(ResponseException.class, () -> executeWithBounds(query, "ts", FROM, TO));
      assertTrue(e.getMessage(), e.getMessage().contains("Field [other] not found."));
    } finally {
      client().performRequest(new Request("DELETE", "/" + noTimeField));
    }
  }

  private JSONObject executeWithBounds(String query, String field, String from, String to)
      throws IOException {
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
    request.setOptions(options);
    Response response = client().performRequest(request);
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }

  private void setPruning(boolean enabled) throws IOException {
    updateClusterSettings(
        new ClusterSetting(
            "persistent",
            Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(),
            Boolean.toString(enabled)));
  }

  /**
   * Clears the override rather than pinning it false: pruning is on by default since #5759, so
   * leaving a false behind would silently disable it for every later class sharing this cluster.
   */
  private void resetPruningToDefault() throws IOException {
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), null));
  }
}
