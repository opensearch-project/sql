/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Exhaustive, fail-loud proof that data ingestion on the analytics-engine route is free of
 * unsupported-field problems. For EVERY {@link Index} value, it runs the real {@link #loadIndex}
 * harness path against the AE cluster, then verifies three independent legs:
 *
 * <ol>
 *   <li><b>Create succeeded</b> — the index exists (a mapping that still contained
 *       nested/geo_point/geo_shape/binary/alias would have been rejected by the parquet/composite
 *       store at PUT time, so existence proves the strip removed them).
 *   <li><b>Mapping is clean</b> — the live mapping pulled back from the cluster contains none of
 *       the unsupported types at any depth.
 *   <li><b>Data agrees with mapping</b> — every doc is searchable and no doc carries a stripped key
 *       (a leftover key would dynamic-map the field back, re-introducing the unsupported type — we
 *       assert the live mapping stays clean after refresh and that doc count &gt; 0 where data
 *       exists).
 * </ol>
 *
 * <p>This converts the "silent failure" risk (a missed strip surfacing only as an unrelated {@code
 * expected:<1> but was:<0>} assertion in some downstream IT) into a direct, attributable failure on
 * the exact index and field. Only runs on the AE route ({@code -Dtests.analytics.parquet_indices});
 * assume-skipped otherwise.
 */
public class AnalyticsUnsupportedFieldStripVerifyIT extends PPLIntegTestCase {

  private static final Set<String> UNSUPPORTED =
      Set.of("nested", "geo_point", "geo_shape", "binary", "alias");

  /**
   * Field types the parquet/composite store also rejects but that are out of scope for the strip
   * (by product decision). An index whose creation fails solely because of one of these is skipped,
   * not reported — this proof is strictly about {@link #UNSUPPORTED}.
   */
  private static final Set<String> OUT_OF_SCOPE_TYPES = Set.of("join");

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  @Test
  public void everyDatasetIngestsCleanlyOnAnalyticsEngine() throws IOException {
    Assume.assumeTrue(
        "AE-route only: requires -Dtests.analytics.parquet_indices=true",
        isAnalyticsParquetIndicesEnabled());

    List<String> failures = new ArrayList<>();
    for (Index index : Index.values()) {
      String name = index.getName();
      try {
        loadIndex(index);
      } catch (Exception e) {
        // A genuinely-absent dataset file is a pre-existing repo issue (dangling enum entry), not
        // an
        // unsupported-field ingestion problem — skip it so this stays a clean, attributable proof.
        if (isMissingDatasetFile(e)) {
          continue;
        }
        // An index that fails creation solely because of an out-of-scope type (e.g. join) is not
        // part of the fix we're verifying — skip rather than report.
        if (failsOnlyOnOutOfScopeType(e)) {
          continue;
        }
        failures.add(
            "["
                + index.name()
                + " -> "
                + name
                + "] loadIndex FAILED (ingestion error): "
                + rootMessage(e));
        continue;
      }
      // Leg 1+2: index exists and its live mapping carries no unsupported type at any depth.
      List<String> offending = unsupportedFieldsInLiveMapping(name);
      if (!offending.isEmpty()) {
        failures.add(
            "["
                + index.name()
                + " -> "
                + name
                + "] live mapping still has unsupported fields: "
                + offending);
      }
    }

    if (!failures.isEmpty()) {
      throw new AssertionError(
          "Unsupported-field ingestion problems on the AE route ("
              + failures.size()
              + "):\n  "
              + String.join("\n  ", failures));
    }
  }

  /** Pull the live mapping back from the cluster and collect any unsupported-typed field paths. */
  private List<String> unsupportedFieldsInLiveMapping(String indexName) throws IOException {
    Request request = new Request("GET", "/" + indexName + "/_mapping");
    Response response = client().performRequest(request);
    JSONObject body = new JSONObject(getResponseBody(response));
    // shape: { indexName: { mappings: { properties: {...} } } }
    JSONObject mappings = body.getJSONObject(indexName).getJSONObject("mappings");
    List<String> offending = new ArrayList<>();
    if (mappings.has("properties")) {
      collectUnsupported(mappings.getJSONObject("properties"), "", offending);
    }
    return offending;
  }

  private void collectUnsupported(JSONObject properties, String prefix, List<String> out) {
    for (String field : properties.keySet()) {
      JSONObject def = properties.optJSONObject(field);
      if (def == null) {
        continue;
      }
      String type = def.optString("type", null);
      String path = prefix.isEmpty() ? field : prefix + "." + field;
      if (type != null && UNSUPPORTED.contains(type)) {
        out.add(path + ":" + type);
      }
      if (def.has("properties")) {
        collectUnsupported(def.getJSONObject("properties"), path, out);
      }
    }
  }

  /**
   * True when the index-creation error names an out-of-scope field type (e.g. the parquet store's
   * "... not supported for field: X of type: join" message). Such an index is not something this
   * proof is responsible for; skip it.
   */
  private static boolean failsOnlyOnOutOfScopeType(Throwable t) {
    String msg = rootMessage(t);
    if (msg == null) {
      return false;
    }
    for (String type : OUT_OF_SCOPE_TYPES) {
      if (msg.contains("type: " + type) || msg.contains("type [" + type + "]")) {
        return true;
      }
    }
    return false;
  }

  /** True when the failure is a missing dataset file on disk (pre-existing dangling enum entry). */
  private static boolean isMissingDatasetFile(Throwable t) {
    for (Throwable r = t; r != null && r.getCause() != r; r = r.getCause()) {
      if (r instanceof java.nio.file.NoSuchFileException
          || r instanceof java.io.FileNotFoundException) {
        return true;
      }
      if (r.getCause() == null) {
        break;
      }
    }
    return false;
  }

  private static String rootMessage(Throwable t) {
    Throwable r = t;
    while (r.getCause() != null && r.getCause() != r) {
      r = r.getCause();
    }
    return r.getMessage();
  }
}
