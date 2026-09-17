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

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
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

  // ==================== PPL: mvzip ====================

  // ==================== PPL: split (string -> array) ====================

  // ==================== PPL: lambda predicates (exists / forall / filter) ====================

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
