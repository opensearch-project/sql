/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.getResponseBody;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Tests that relevance search functions which cannot be pushed down fail fast on the coordinator
 * with an actionable message, instead of being serialized into a script that only fails when
 * compiled on the shards.
 *
 * <p>Relevance functions have no row-by-row implementation -- they exist only to be rewritten into
 * an OpenSearch query during push down. Before this fix, a query the analyzer could not handle was
 * wrapped in a script and shipped to every data node, surfacing as {@code QueryShardException:
 * Failed to compile inline script} with all shards failing.
 */
public class CalciteRelevanceFunctionPushdownFailureIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "relevance_pushdown_failure";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    createTestIndex();
  }

  private void createTestIndex() throws IOException {
    if (isIndexExist(client(), TEST_INDEX)) {
      return;
    }
    createIndexByRestClient(
        client(),
        TEST_INDEX,
        "{\"mappings\":{\"properties\":{\"body\":{\"type\":\"text\"},"
            + "\"idx\":{\"type\":\"integer\"}}}}");
    Request bulk = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{\"_id\":\"1\"}}\n"
            + "{\"body\":\"ERROR something bad happened\",\"idx\":1}\n"
            + "{\"index\":{\"_id\":\"2\"}}\n"
            + "{\"body\":\"INFO all good\",\"idx\":2}\n");
    performRequest(client(), bulk);
  }

  private String errorOf(String query) throws IOException {
    ResponseException e =
        assertThrows(ResponseException.class, () -> executeQuery(query));
    return getResponseBody(e.getResponse());
  }

  private void assertFailsCleanly(String query, String expectedFunctionName)
      throws IOException {
    String message = errorOf(query);
    assertFalse(
        "Relevance function must not be pushed down as a script; it cannot compile on the shard."
            + " Error was: "
            + message,
        message.contains("Failed to compile inline script"));
    assertFalse(
        "Query must fail on the coordinator, not as a shard failure. Error was: " + message,
        message.contains("all shards failed"));
    assertTrue(
        String.format(
            Locale.ROOT, "Error should name the function [%s]. Error was: %s", "match", message),
        message.toLowerCase(Locale.ROOT).contains(expectedFunctionName));
  }

  /** A relevance function over a column computed by the query cannot be pushed down. */
  @Test
  public void relevanceOnEvalDerivedColumnFailsCleanly() throws IOException {
    assertFailsCleanly(
        String.format(
            Locale.ROOT,
            "source=%s | eval b2 = upper(body) | where match(b2, 'ERROR') | fields idx",
            TEST_INDEX),
        "match");
  }

  /** Same for a column produced by parse. */
  @Test
  public void relevanceOnParseDerivedColumnFailsCleanly() throws IOException {
    assertFailsCleanly(
        String.format(
            Locale.ROOT,
            "source=%s | parse body '(?<lvl>\\\\w+)' | where match(lvl, 'ERROR') | fields idx",
            TEST_INDEX),
        "match");
  }

  /** A relevance filter above an aggregation has no scan to be pushed onto. */
  @Test
  public void relevanceAboveAggregationFailsCleanly() throws IOException {
    assertFailsCleanly(
        String.format(Locale.ROOT, "source=%s | top 1 body | where match(body, 'ERROR')", TEST_INDEX),
        "match");
  }

  /** A relevance function in a projection is never rewritten into a query. */
  @Test
  public void relevanceInProjectionFailsCleanly() throws IOException {
    assertFailsCleanly(
        String.format(
            Locale.ROOT, "source=%s | eval m = match(body, 'ERROR') | fields idx, m", TEST_INDEX),
        "match");
  }

  /**
   * Regression guard: the pattern from the customer report -- a relevance filter combined with a
   * LIKE filter on a pure text field -- must keep working. The LIKE goes down as a script while the
   * relevance function goes down as a native match query.
   */
  @Test
  public void relevanceCombinedWithLikeStillWorks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | where match(body, 'ERROR') | where like(body, '%%error%%') | fields"
                    + " idx",
                TEST_INDEX));
    verifyDataRows(result, rows(1));
  }

  /** Regression guard: a plain relevance filter on an indexed field is unaffected. */
  @Test
  public void relevanceOnIndexedFieldStillWorks() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                Locale.ROOT, "source=%s | where match(body, 'ERROR') | fields idx", TEST_INDEX));
    verifyDataRows(result, rows(1));
  }
}
