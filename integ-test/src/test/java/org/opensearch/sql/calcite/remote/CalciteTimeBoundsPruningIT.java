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
 * The {@code time_range} request field: bounds of a filter the query text already carries, sent so
 * the engine can drop indices that cannot hold data in the range <em>before</em> it resolves the
 * queried expression and merges every matched index's mapping.
 *
 * <p>Pruning is observed through the resolved schema rather than through timings: a field that only
 * the out-of-range index maps stops resolving once that index is gone, which is both the cheapest
 * signal and the one that matters -- the merge is what a wildcard over months of rollovers actually
 * pays for.
 */
public class CalciteTimeBoundsPruningIT extends PPLIntegTestCase {

  private static final String OLD_INDEX = "prune_range_000001";
  private static final String NEW_INDEX = "prune_range_000002";
  private static final String PATTERN = "prune_range_*";

  /** Covers only {@link #NEW_INDEX}'s documents. */
  private static final String FROM = "2026-09-10 00:00:00.000";

  private static final String TO = "2026-09-10 23:59:59.999";

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
    setPruning(false);
  }

  /**
   * A rollover that dropped a field: {@code legacy_only} exists in the older index and not the
   * newer, so whether it resolves says exactly which indices the merge saw.
   */
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
  public void shouldResolveTheWholePatternWithoutAHint() throws IOException {
    verifyDataRows(executeQuery(IN_RANGE + " | fields legacy_only | head 1"), rows((Object) null));
  }

  /** The filter still does the filtering, so pruning must not change a single row. */
  @Test
  public void shouldReturnTheSameRowsWithAndWithoutTheHint() throws IOException {
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
  public void shouldIgnoreTheHintWhenPruningIsDisabled() throws IOException {
    setPruning(false);

    verifyDataRows(
        executeWithBounds(IN_RANGE + " | fields legacy_only | head 1", "ts", FROM, TO),
        rows((Object) null));
  }

  /**
   * The hint only decides which indices are read, so an unusable one is dropped rather than failing
   * a query that does not need it.
   */
  @Test
  public void shouldIgnoreAnUnusableHint() throws IOException {
    verifyDataRows(
        executeWithBounds(IN_RANGE + " | stats count()", "ts", "Invalid date", TO), rows(2));
    verifyDataRows(executeWithBounds(IN_RANGE + " | stats count()", "", FROM, TO), rows(2));
    // Inverted: nothing could match it, which is a sign the client got it wrong, not a range.
    verifyDataRows(executeWithBounds(IN_RANGE + " | stats count()", "ts", TO, FROM), rows(2));
  }

  /** Naming a field no index maps must leave the expression alone rather than prune it away. */
  @Test
  public void shouldIgnoreAHintOnAnUnmappedField() throws IOException {
    verifyDataRows(
        executeWithBounds(IN_RANGE + " | stats count()", "no_such_field", FROM, TO), rows(2));
  }

  /**
   * The bounds describe the window the caller is asking about, so they reach a subsearch's source
   * too -- the same scope Splunk's time range picker and ES|QL's request-level filter have. Here
   * the subsearch reads the out-of-range index directly, so global scope prunes it to nothing and
   * it contributes no rows.
   */
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

    // Without the bounds the subsearch would find its one row in the older index.
    verifyDataRows(executeQuery(query), rows(1));
    // With them, the older index is not read at all, so the subsearch aggregates nothing and the
    // join drops the row.
    assertEquals(0, executeWithBounds(query, "ts", FROM, TO).getInt("total"));
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
}
