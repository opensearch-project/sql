/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.Capability.TIME_BOUNDS_PRUNING;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.TestUtils.createIndexByRestClient;
import static org.opensearch.sql.util.TestUtils.isIndexExist;
import static org.opensearch.sql.util.TestUtils.performRequest;

import java.io.IOException;
import java.util.Locale;
import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Request-level time bounds must not change join or lookup semantics. Bounds only drop whole
 * indices from the outermost {@code source=}; a join's other side, a lookup table and the rows of a
 * kept index are untouched.
 *
 * <p>Fixtures: {@code A} and {@code B} share timestamps (one row in range, one out), {@code C} has
 * only out-of-range timestamps, {@code D} does not map the time field. Each has keys 1 and 2.
 */
@RequiresCapability(TIME_BOUNDS_PRUNING)
public class CalciteTimeBoundsJoinIT extends PPLIntegTestCase {

  private static final String A = "prune_join_a";
  private static final String B = "prune_join_b";
  private static final String C = "prune_join_c";
  private static final String D = "prune_join_d";

  private static final String FROM = "2026-09-10 00:00:00.000";
  private static final String TO = "2026-09-10 23:59:59.999";
  private static final String IN = "2026-09-10T10:00:00Z";
  private static final String OUT = "2026-06-01T10:00:00Z";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    seed(A, "va", IN, OUT);
    seed(B, "vb", IN, OUT);
    seed(C, "vc", OUT, OUT);
    seed(D, "vd", null, null);
  }

  private void seed(String index, String valueField, String ts1, String ts2) throws IOException {
    if (isIndexExist(client(), index)) {
      return;
    }
    String tsMapping = ts1 == null ? "" : "\"ts\":{\"type\":\"date\"},";
    createIndexByRestClient(
        client(),
        index,
        "{\"mappings\":{\"properties\":{"
            + tsMapping
            + "\"k\":{\"type\":\"integer\"},\""
            + valueField
            + "\":{\"type\":\"keyword\"}}}}");
    Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
    bulk.setJsonEntity(doc(valueField, ts1, 1) + doc(valueField, ts2, 2));
    performRequest(client(), bulk);
  }

  private static String doc(String valueField, String ts, int k) {
    String tsField = ts == null ? "" : "\"ts\":\"" + ts + "\",";
    return "{\"index\":{}}\n{"
        + tsField
        + "\"k\":"
        + k
        + ",\""
        + valueField
        + "\":\""
        + valueField
        + k
        + "\"}\n";
  }

  /** Same rows with and without bounds. */
  @SafeVarargs
  private void assertUnchanged(String query, Matcher<JSONArray>... expected) throws IOException {
    verifyDataRows(executeQuery(query), expected);
    verifyDataRows(executeWithBounds(query), expected);
  }

  // --- both sides map ts, shared timestamps ---

  @Test
  public void joinBothSidesInRange() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | join left=l right=r on l.k = r.k [ source="
            + B
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void joinOnTheTimeFieldItself() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | join left=l right=r on l.ts = r.ts [ source="
            + B
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void selfJoin() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | join left=l right=r on l.k = r.k [ source="
            + A
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void joinWithClientTimeFilterOnBothSides() throws IOException {
    String window = " | where ts >= '" + FROM + "' and ts <= '" + TO + "'";
    assertUnchanged(
        "source="
            + A
            + window
            + " | join left=l right=r on l.k = r.k [ source="
            + B
            + window
            + " ] | stats count() as n",
        rows(1));
  }

  // --- right side out of range / unmapped: must never be pruned ---

  @Test
  public void joinRightSideOutOfRange() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | join left=l right=r on l.k = r.k [ source="
            + C
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void joinRightSideHasNoTimeField() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | join left=l right=r on l.k = r.k [ source="
            + D
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void joinRightSideBareTableName() throws IOException {
    assertUnchanged(
        "source=" + A + " | join left=l right=r on l.k = r.k " + C + " | stats count() as n",
        rows(2));
  }

  @Test
  public void joinRightSideMultiIndexSpanningTheRange() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | head 1 | eval k = 1 | join left=l right=r on l.k = r.k [ source="
            + A
            + ","
            + B
            + ","
            + C
            + " | stats count() as n by k ] | fields n",
        rows(3));
  }

  @Test
  public void joinCriteriaQualifiedByIndexName() throws IOException {
    assertUnchanged(
        "source=" + A + " | join on " + A + ".k = " + B + ".k " + B + " | stats count() as n",
        rows(2));
  }

  // --- lookup ---

  @Test
  public void lookupTableOutOfRange() throws IOException {
    assertUnchanged(
        "source=" + A + " | lookup " + C + " k append vc | sort k | fields k, vc",
        rows(1, "vc1"),
        rows(2, "vc2"));
  }

  @Test
  public void lookupTableWithoutTimeField() throws IOException {
    assertUnchanged(
        "source=" + A + " | lookup " + D + " k append vd | sort k | fields k, vd",
        rows(1, "vd1"),
        rows(2, "vd2"));
  }

  @Test
  public void lookupTableSharingTimestamps() throws IOException {
    assertUnchanged(
        "source=" + A + " | lookup " + B + " k append vb | sort k | fields k, vb",
        rows(1, "vb1"),
        rows(2, "vb2"));
  }

  // --- concrete main-source names: IndexPruner declines them, so nothing changes ---

  @Test
  public void concreteMainSourceListIsNotPruned() throws IOException {
    assertUnchanged(
        "source="
            + A
            + ","
            + C
            + " | join left=l right=r on l.k = r.k [ source="
            + B
            + " ]"
            + " | stats count() as n",
        rows(4));
  }

  @Test
  public void concreteMainSourceOutOfRangeIsNotPruned() throws IOException {
    assertUnchanged(
        "source="
            + C
            + " | join left=l right=r on l.k = r.k [ source="
            + A
            + " ]"
            + " | stats count() as n",
        rows(2));
  }

  @Test
  public void concreteMainSourceWithoutTimeFieldIsNotPruned() throws IOException {
    assertUnchanged(
        "source=" + D + " | lookup " + A + " k append va | stats count() as n", rows(2));
  }

  // --- wildcard main sources: the only shape IndexPruner narrows ---

  @Test
  public void wildcardMainSourceDropsOnlyTheOutOfRangeIndex() throws IOException {
    String query =
        "source="
            + A
            + "*,"
            + C
            + "* | join left=l right=r on l.k = r.k [ source="
            + B
            + " ]"
            + " | stats count() as n";
    verifyDataRows(executeQuery(query), rows(4));
    verifyDataRows(executeWithBounds(query), rows(2));
  }

  @Test
  public void wildcardRightSideIsNotPruned() throws IOException {
    assertUnchanged(
        "source="
            + A
            + " | head 1 | eval k = 1 | join left=l right=r on l.k = r.k [ source="
            + "prune_join_* | stats count() as n by k ] | fields n",
        rows(4));
  }

  @Test
  public void wildcardLookupMainSourceKept() throws IOException {
    assertUnchanged(
        "source=" + A + "* | lookup " + C + " k append vc | sort k | fields k, vc",
        rows(1, "vc1"),
        rows(2, "vc2"));
  }

  @Test
  public void wildcardMainSourceOutOfRange() throws IOException {
    String query =
        "source="
            + C
            + "* | join left=l right=r on l.k = r.k [ source="
            + A
            + " ]"
            + " | stats count() as n";
    verifyDataRows(executeQuery(query), rows(2));
    record("wildcardMainSourceOutOfRange", query);
  }

  @Test
  public void wildcardMainSourceWithoutTimeField() throws IOException {
    String query =
        "source="
            + D
            + "* | join left=l right=r on l.k = r.k [ source="
            + A
            + " ]"
            + " | stats count() as n";
    verifyDataRows(executeQuery(query), rows(2));
    record("wildcardMainSourceWithoutTimeField", query);
  }

  @Test
  public void wildcardMainSourceWithoutTimeFieldLookup() throws IOException {
    String query = "source=" + D + "* | lookup " + A + " k append va | stats count() as n";
    verifyDataRows(executeQuery(query), rows(2));
    record("wildcardMainSourceWithoutTimeFieldLookup", query);
  }

  /** Not join-specific: any field qualified by the main source's index name. */
  @Test
  public void fieldQualifiedByIndexNameWithoutJoin() throws IOException {
    assertUnchanged("source=" + A + " | where " + A + ".k = 1 | stats count() as n", rows(1));
  }

  @Test
  public void fieldQualifiedByExplicitAlias() throws IOException {
    assertUnchanged(
        "source=" + A + " as t | join on t.k = r.k " + B + " as r | stats count() as n", rows(2));
  }

  private void record(String name, String query) {
    String outcome;
    try {
      outcome = executeWithBounds(query).getJSONArray("datarows").toString();
    } catch (Exception e) {
      outcome = "ERROR " + e.getMessage();
    }
    System.out.println("PRUNE_JOIN_RECORD " + name + " => " + outcome);
  }

  private JSONObject executeWithBounds(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        String.format(
            Locale.ROOT,
            "{ \"query\": \"%s\", \"time_field\": \"ts\", \"start_time\": \"%s\","
                + " \"end_time\": \"%s\" }",
            query,
            FROM,
            TO));
    RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
    options.addHeader("Content-Type", "application/json");
    request.setOptions(options);
    Response response = client().performRequest(request);
    return new JSONObject(org.opensearch.sql.legacy.TestUtils.getResponseBody(response, true));
  }
}
