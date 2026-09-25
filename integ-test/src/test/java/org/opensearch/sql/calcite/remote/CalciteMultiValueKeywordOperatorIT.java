/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRowsInOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for the EXPLICIT multi-value/array operators on a real {@code multi_value}
 * keyword field, executed against the analytics engine (composite/parquet). Complements {@code
 * CalciteArrayFunctionIT} (inline {@code array()} literals) by exercising operators against an
 * actually-declared multi_value field.
 *
 * <p>Operators route to their correct frontend: {@code array_*}/{@code cardinality}/subscript are
 * SQL (SQL endpoint); {@code mv*}/{@code array_length} and the {@code mvexpand} command are PPL.
 * Implicit scalar-op-on-array behavior is NOT tested — explicit-only.
 *
 * <p>Assertions are EXACT: filters assert the precise document id set (count + identity together,
 * so a right-count/wrong-docs result fails), and value/projection tests assert the exact returned
 * value/array via {@code verifyDataRows} (not substring or bare-count checks) so an error response
 * or empty/zero-doc result cannot false-pass.
 */
public class CalciteMultiValueKeywordOperatorIT extends PPLIntegTestCase {

  private static final String INDEX = "mv_kw_ops";
  private static final String DYNAMIC_INDEX = "mv_kw_dynamic";

  @Override
  public void init() throws Exception {
    super.init();
    // NOTE: intentionally NOT calling enableCalcite() — the analytics-engine path requires the
    // Calcite engine, and this IT verifies the feature works with the cluster's default settings
    // (mirroring a customer who never flips plugins.calcite.enabled). If Calcite is not default-on
    // for the target version this will surface here rather than being masked by an explicit opt-in.
    provisionMultiValueIndex();
  }

  private void provisionMultiValueIndex() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + INDEX));
    } catch (Exception ignored) {
    }
    String mapping =
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"index.pluggable.dataformat.enabled\":true,"
            + "\"index.pluggable.dataformat\":\"composite\","
            + "\"index.composite.primary_data_format\":\"parquet\","
            + "\"index.composite.secondary_data_formats\":[\"lucene\"]},"
            + "\"mappings\":{\"properties\":{"
            + "\"id\":{\"type\":\"keyword\"},"
            + "\"tags\":{\"type\":\"keyword\",\"multi_value\":true}}}}";
    Request create = new Request("PUT", "/" + INDEX);
    create.setJsonEntity(mapping);
    client().performRequest(create);

    Request health = new Request("GET", "/_cluster/health/" + INDEX);
    health.addParameter("wait_for_status", "green");
    health.addParameter("timeout", "30s");
    client().performRequest(health);

    // Explicit multi_value mapping: every document supplies an ARRAY (single-element for single
    // values), so every parquet file stores tags as LIST<keyword>. No hybrid scalar+LIST shard.
    // Fixture:
    //   d1 -> [prod]        d2 -> [blue]
    //   d3 -> [prod, blue]  d4 -> [green, prod, green]
    bulk(
        "{\"index\":{}}\n{\"id\":\"d1\",\"tags\":[\"prod\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d2\",\"tags\":[\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d3\",\"tags\":[\"prod\",\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d4\",\"tags\":[\"green\",\"prod\",\"green\"]}\n");
    client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
  }

  private void bulk(String body) throws IOException {
    Request r = new Request("POST", "/" + INDEX + "/_bulk");
    r.setJsonEntity(body);
    r.addParameter("refresh", "true");
    client().performRequest(r);
  }

  /**
   * Provisions a composite/parquet index with NO explicit {@code multi_value} mapping for {@code
   * tags}. The field is dynamically mapped, and because the first document supplies multiple values
   * the pluggable-dataformat parser auto-promotes it to a {@code multi_value} (LIST) field. This
   * exercises the dynamic-mapping path a customer hits when they index arrays without declaring the
   * field up front.
   */
  private void provisionDynamicIndex() throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + DYNAMIC_INDEX));
    } catch (Exception ignored) {
    }
    // Composite/parquet settings are required for the analytics-engine path and for multi_value
    // auto-promotion, but NO "properties" mapping is declared — tags is dynamically mapped.
    String settings =
        "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0,"
            + "\"index.pluggable.dataformat.enabled\":true,"
            + "\"index.pluggable.dataformat\":\"composite\","
            + "\"index.composite.primary_data_format\":\"parquet\","
            + "\"index.composite.secondary_data_formats\":[\"lucene\"]}}";
    Request create = new Request("PUT", "/" + DYNAMIC_INDEX);
    create.setJsonEntity(settings);
    client().performRequest(create);

    Request health = new Request("GET", "/_cluster/health/" + DYNAMIC_INDEX);
    health.addParameter("wait_for_status", "green");
    health.addParameter("timeout", "30s");
    client().performRequest(health);

    // First doc supplies MULTIPLE values so the pluggable parser promotes tags to LIST
    // (multi_value)
    // on dynamic mapping. Same fixture shape as the explicit index so operator assertions match.
    Request r = new Request("POST", "/" + DYNAMIC_INDEX + "/_bulk");
    r.setJsonEntity(
        "{\"index\":{}}\n{\"id\":\"d3\",\"tags\":[\"prod\",\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d1\",\"tags\":[\"prod\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d2\",\"tags\":[\"blue\"]}\n"
            + "{\"index\":{}}\n{\"id\":\"d4\",\"tags\":[\"green\",\"prod\",\"green\"]}\n");
    r.addParameter("refresh", "true");
    client().performRequest(r);
    client().performRequest(new Request("POST", "/" + DYNAMIC_INDEX + "/_flush?force=true"));
  }

  /** Returns the {@code _mapping} of the given index as a parsed JSON object. */
  private JSONObject getMapping(String index) throws IOException {
    Request req = new Request("GET", "/" + index + "/_mapping");
    org.opensearch.client.Response resp = client().performRequest(req);
    return new JSONObject(
        new String(
            resp.getEntity().getContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
  }

  private JSONObject ppl(String query) throws IOException {
    return executeQuery(query);
  }

  // ==================== PPL: projection ====================

  @Test
  public void testPplProjectionArrayDoc() throws IOException {
    JSONObject r = ppl(String.format("source=%s | where id='d3' | fields id, tags", INDEX));
    verifySchema(r, schema("id", "string"), schema("tags", "array"));
    verifyDataRows(r, rows("d3", List.of("prod", "blue")));
  }

  @Test
  public void testPplProjectionSingleValueDoc() throws IOException {
    JSONObject r = ppl(String.format("source=%s | where id='d1' | fields id, tags", INDEX));
    verifyDataRows(r, rows("d1", List.of("prod")));
  }

  // ==================== PPL: array_length ====================

  @Test
  public void testPplArrayLength() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d4' | eval n = array_length(tags) | fields n", INDEX));
    verifyDataRows(r, rows(3));
  }

  @Test
  public void testPplArrayLengthFilterByCountUseCase() throws IOException {
    JSONObject r =
        ppl(String.format("source=%s | where array_length(tags) > 1 | sort id | fields id", INDEX));
    // Only multi-element docs: d3 [prod,blue], d4 [green,prod,green].
    verifyDataRowsInOrder(r, rows("d3"), rows("d4"));
  }

  // ==================== PPL: mvjoin ====================

  @Test
  public void testPplMvjoin() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | eval s = mvjoin(tags, '-') | fields s", INDEX));
    verifyDataRows(r, rows("prod-blue"));
  }

  // ==================== PPL: mvindex ====================

  @Test
  public void testPplMvindex() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | eval first = mvindex(tags, 0) | fields first", INDEX));
    verifyDataRows(r, rows("prod"));
  }

  // ==================== PPL: mvfind (index + contains use case) ====================

  @Test
  public void testPplMvfind() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | eval idx = mvfind(tags, 'blue') | fields idx", INDEX));
    verifyDataRows(r, rows(1)); // 0-based index of 'blue' in [prod, blue]
  }

  @Test
  public void testPplMvfindContainsUseCase() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where mvfind(tags, 'blue') >= 0 | sort id | fields id", INDEX));
    // Docs whose tags contain 'blue': d2 [blue], d3 [prod,blue].
    verifyDataRowsInOrder(r, rows("d2"), rows("d3"));
  }

  // ==================== PPL: mvdedup ====================

  @Test
  public void testPplMvdedup() throws IOException {
    JSONObject r =
        ppl(String.format("source=%s | where id='d4' | eval u = mvdedup(tags) | fields u", INDEX));
    // d4 tags = [green, prod, green] -> deduped, first-occurrence order.
    verifyDataRows(r, rows(List.of("green", "prod")));
  }

  // ==================== PPL: mvappend ====================

  @Test
  public void testPplMvappend() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d2' | eval a = mvappend(tags, 'extra') | fields a", INDEX));
    verifyDataRows(r, rows(List.of("blue", "extra")));
  }

  // ==================== PPL: mvexpand command (+ group-by use case) ====================

  @Test
  public void testPplMvexpand() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | mvexpand tags | sort tags | fields id, tags", INDEX));
    // d3 [prod, blue] explodes to two rows, sorted by tags: blue, prod.
    verifyDataRowsInOrder(r, rows("d3", "blue"), rows("d3", "prod"));
  }

  @Test
  public void testPplMvexpandGroupByUseCase() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | mvexpand tags | stats count() as c by tags | sort tags", INDEX));
    // Per-element counts across all docs, sorted by tag:
    //   blue: d2, d3            = 2
    //   green: d4 (x2, mvexpand does not dedup) = 2
    //   prod: d1, d3, d4        = 3
    verifyDataRowsInOrder(r, rows(2, "blue"), rows(2, "green"), rows(3, "prod"));
  }

  // ==================== PPL: implicit group-by on a multi_value field ====================
  // Unlike testPplMvexpandGroupByUseCase (which explicitly expands first), these group directly by
  // the multi_value field with NO mvexpand. The analytics engine expands array elements into
  // per-element buckets, so each element of every document's tags contributes to its own bucket.
  // Element occurrences: prod -> d1,d3,d4 (3); blue -> d2,d3 (2); green -> d4 twice (2).

  @Test
  public void testPplImplicitGroupByMv() throws IOException {
    JSONObject r = ppl(String.format("source=%s | stats count() as c by tags | sort tags", INDEX));
    verifyDataRowsInOrder(r, rows(2, "blue"), rows(2, "green"), rows(3, "prod"));
  }

  @Test
  public void testPplImplicitGroupByMvDistinctCount() throws IOException {
    // Distinct documents per element (green occurs twice in d4 but is one distinct doc).
    JSONObject r =
        ppl(String.format("source=%s | stats dc(id) as docs by tags | sort tags", INDEX));
    verifyDataRowsInOrder(r, rows(2, "blue"), rows(1, "green"), rows(3, "prod"));
  }

  // ==================== PPL: mvzip ====================

  @Test
  public void testPplMvzip() throws IOException {
    // Zip the multi_value field against an inline array positionally. d3 -> [prod, blue].
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | eval z = mvzip(tags, array('a', 'b')) | fields z",
                INDEX));
    verifyDataRows(r, rows(List.of("prod,a", "blue,b")));
  }

  @Test
  public void testPplMvzipCustomDelimiter() throws IOException {
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d3' | eval z = mvzip(tags, array('a', 'b'), '|') | fields z",
                INDEX));
    verifyDataRows(r, rows(List.of("prod|a", "blue|b")));
  }

  // ==================== PPL: split (string -> array) ====================

  @Test
  public void testPplSplit() throws IOException {
    // split() produces an array from a scalar string; independent of the mv field but exercises the
    // array-producing path on the AE route.
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d1' | eval parts = split('a-b-c', '-') | fields parts",
                INDEX));
    verifyDataRows(r, rows(List.of("a", "b", "c")));
  }

  // ==================== PPL: lambda predicates (exists / forall / filter) ====================
  // NOTE: higher-order lambda functions (exists/forall/filter/transform/reduce) are NOT supported
  // on the analytics-engine route — the backend rejects them with "Function [exists] is not
  // currently supported as a scalar function". CalciteArrayFunctionIT gates them behind
  // @RequiresCapability(ARRAY_HIGHER_ORDER_FUNC) for the non-AE engine. They are intentionally not
  // exercised here; enabling lambda predicates on the AE path is a separate feature, not part of
  // multi_value keyword support.

  // ==================== Dynamic mapping (no explicit multi_value) ====================
  // A customer indexes array documents with NO declared mapping; the field must auto-promote to
  // multi_value and behave identically to an explicitly-declared one.

  @Test
  public void testDynamicMappingReportsMultiValue() throws IOException {
    provisionDynamicIndex();
    JSONObject mapping = getMapping(DYNAMIC_INDEX);
    JSONObject tags =
        mapping
            .getJSONObject(DYNAMIC_INDEX)
            .getJSONObject("mappings")
            .getJSONObject("properties")
            .getJSONObject("tags");
    // The dynamically-mapped field must carry multi_value:true after ingesting multi-element
    // arrays.
    org.junit.jupiter.api.Assertions.assertTrue(
        tags.optBoolean("multi_value", false),
        "dynamically-mapped tags should be multi_value:true, mapping was: " + tags);
  }

  @Test
  public void testDynamicMappingProjectionReturnsArray() throws IOException {
    provisionDynamicIndex();
    JSONObject r = ppl(String.format("source=%s | where id='d3' | fields tags", DYNAMIC_INDEX));
    // A projected multi_value field returns the array value, not a scalar.
    verifyDataRows(r, rows(List.of("prod", "blue")));
  }

  @Test
  public void testDynamicMappingArrayLength() throws IOException {
    provisionDynamicIndex();
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d4' | eval n = array_length(tags) | fields n",
                DYNAMIC_INDEX));
    // d4 -> [green, prod, green] has 3 elements.
    verifyDataRows(r, rows(3));
  }

  @Test
  public void testDynamicMappingImplicitGroupBy() throws IOException {
    provisionDynamicIndex();
    JSONObject r =
        ppl(String.format("source=%s | stats count() as c by tags | sort tags", DYNAMIC_INDEX));
    verifyDataRowsInOrder(r, rows(2, "blue"), rows(2, "green"), rows(3, "prod"));
  }

  // ==================== PPL: mvexpand edge cases ====================

  @Test
  public void testPplMvexpandLimit() throws IOException {
    // mvexpand ... limit=N caps elements PER DOCUMENT. d4 [green, prod, green] limit=2 -> 2 rows.
    JSONObject r =
        ppl(
            String.format(
                "source=%s | where id='d4' | mvexpand tags limit=2 | sort tags | fields id, tags",
                INDEX));
    // First two elements of [green, prod, green] are green, prod -> sorted: green, prod.
    verifyDataRowsInOrder(r, rows("d4", "green"), rows("d4", "prod"));
  }

  @Test
  public void testPplMvexpandSingleElement() throws IOException {
    // Single-element array expands to exactly one row.
    JSONObject r =
        ppl(String.format("source=%s | where id='d1' | mvexpand tags | fields id, tags", INDEX));
    verifyDataRowsInOrder(r, rows("d1", "prod"));
  }
}
