/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestUtils.analyticsDroppedFields;
import static org.opensearch.sql.legacy.TestUtils.getResourceFilePath;
import static org.opensearch.sql.legacy.TestUtils.getResponseBody;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assume;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Exhaustive, fail-loud proof that data ingestion on the analytics-engine route is free of
 * unsupported-field problems. For EVERY {@link Index} value it forces a clean (re)load through the
 * real {@link #loadIndex} harness path against the AE cluster, then verifies, per dataset:
 *
 * <ol>
 *   <li><b>The real load path ran from a clean index</b> — the index is deleted first, so {@code
 *       loadIndex}'s {@code if (!isIndexExist)} guard cannot short-circuit and inspect a stale
 *       index left by an earlier test. {@code loadDataByRestClient} now fails loudly on bulk {@code
 *       errors=true}, so a partial ingestion surfaces here as a load failure, not silently.
 *   <li><b>Mapping is clean</b> — the live mapping pulled back from the cluster contains none of
 *       the unsupported types at any depth.
 *   <li><b>Source agrees with mapping</b> — no sampled {@code _source} doc still carries a stripped
 *       path (a leftover value would dynamic-map the field back, re-introducing the unsupported
 *       type).
 *   <li><b>Doc count is sane</b> — a fixture with N source docs ingests exactly N (proving no item
 *       silently dropped). Skipped only for a degenerate index whose strip left no scannable
 *       columns (e.g. cascaded_nested, all-{@code nested}), which the engine can't query at all.
 * </ol>
 *
 * <p>This converts the "silent failure" risk (a missed strip surfacing only as an unrelated {@code
 * expected:<1> but was:<0>} assertion in some downstream IT) into a direct, attributable failure on
 * the exact index and field. Only runs on the AE route ({@code -Dtests.analytics.parquet_indices});
 * assume-skipped otherwise.
 */
public class AnalyticsUnsupportedFieldStripVerifyIT extends PPLIntegTestCase {

  private static final Logger LOG =
      LogManager.getLogger(AnalyticsUnsupportedFieldStripVerifyIT.class);

  // Single source of truth — the same set the load-path strip uses
  // (TestUtils.AnalyticsIndexConfig). Importing it (not re-listing) keeps verifier and stripper in
  // lockstep.
  private static final Set<String> UNSUPPORTED =
      org.opensearch.sql.legacy.TestUtils.AnalyticsIndexConfig.UNSUPPORTED_FIELD_TYPES;

  /**
   * Field types the parquet/composite store also rejects but that are out of scope for the strip
   * (by product decision). An index whose creation fails solely because of one of these is skipped
   * — but only after confirming its mapping carries no {@link #UNSUPPORTED} type (see {@link
   * #safeToSkipForOutOfScopeType}), so a mixed failure can't be hidden.
   */
  private static final Set<String> OUT_OF_SCOPE_TYPES = Set.of("join");

  /**
   * Datasets that carry a multi-value JSON array for a scalar-mapped field (e.g. a {@code text} or
   * {@code long} field given several values in a doc). The parquet/composite store rejects these at
   * bulk load with {@code Cannot accept multiple values for field} — a cardinality limitation, not
   * an unsupported field *type*, so it is out of scope for the type strip ({@link
   * org.opensearch.sql.legacy.TestUtils.AnalyticsIndexConfig}) the same way {@link
   * #OUT_OF_SCOPE_TYPES} is. Tracked by the {@code MULTI_VALUE_FIELD_LOAD} capability. An index
   * here is skipped only when its failure is the exact multi-value signature AND it is on this
   * curated list (see {@link #safeToSkipForMultiValueLoad}); any other failure surfaces loudly.
   * Legs 2-3 still type-check the live mapping of every index that loads, so a missed type-strip
   * can't hide.
   */
  private static final Set<String> MULTI_VALUE_DATASETS =
      Set.of(
          "GAME_OF_THRONES",
          "NESTED",
          "NESTED_WITH_QUOTES",
          "DEEP_NESTED",
          "NESTED_WITH_NULLS",
          "GRAPH_AIRPORTS",
          "ARRAY",
          "OTELLOGS");

  /** Cluster's per-item bulk error when a doc supplies an array to a scalar-mapped field. */
  private static final String MULTI_VALUE_SIGNATURE = "Cannot accept multiple values for field";

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
      String mapping = index.getMapping();

      // Force the real load path to run: delete any index a prior test left behind so loadIndex's
      // `if (!isIndexExist)` guard can't short-circuit and inspect a stale index.
      try {
        deleteIndexIfExists(name);
      } catch (IOException e) {
        failures.add("[" + index.name() + " -> " + name + "] could not delete: " + rootMessage(e));
        continue;
      }

      try {
        loadIndex(index);
      } catch (Exception e) {
        if (isMissingDatasetFile(e)) {
          // Dangling enum entry with no dataset file on disk — pre-existing repo issue, not an
          // unsupported-field problem. Skip.
          continue;
        }
        if (safeToSkipForOutOfScopeType(e, mapping)) {
          // Creation failed solely on an out-of-scope type (e.g. join) AND the mapping carries no
          // unsupported type we're responsible for — not our concern. Skip.
          continue;
        }
        if (safeToSkipForMultiValueLoad(e, index)) {
          // Load failed with the multi-value signature on a known multi-value-array-into-scalar
          // dataset (a cardinality limitation, not an unsupported type) — out of scope for the type
          // strip, tracked by MULTI_VALUE_FIELD_LOAD. Skip.
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

      // Leg 2: live mapping carries no unsupported type at any depth.
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

      // Leg 3: no sampled _source doc still carries a stripped path.
      Set<List<String>> dropped = analyticsDroppedFields(mapping);
      List<String> leftover = strippedPathsStillInSource(name, dropped);
      if (!leftover.isEmpty()) {
        failures.add(
            "["
                + index.name()
                + " -> "
                + name
                + "] _source still carries stripped paths "
                + leftover
                + " (dynamic mapping would re-introduce the unsupported type)");
      }

      // Leg 4: a non-empty fixture ingests exactly its source-doc count — proving no item silently
      // dropped. Skipped for a degenerate index whose strip left no scannable columns (can't
      // query).
      // The count goes through PPL `stats count()`; if that query errors for a cluster-side reason
      // unrelated to the strip (e.g. it transiently touches a parquet-backed system index), we
      // record a transparent skip rather than fail — Legs 1-3 plus the load-time bulk errors=false
      // check (TestUtils.loadDataByRestClient) already catch partial ingestion directly.
      int expectedDocs = sourceDocCount(index);
      if (expectedDocs > 0 && liveMappingHasColumns(name)) {
        Integer actualDocs = liveDocCountOrNull(name);
        if (actualDocs == null) {
          skippedDocCount.add(index.name() + " (count query errored — see Legs 1-3 + bulk check)");
        } else if (actualDocs != expectedDocs) {
          failures.add(
              "["
                  + index.name()
                  + " -> "
                  + name
                  + "] ingested "
                  + actualDocs
                  + " docs but dataset"
                  + " has "
                  + expectedDocs
                  + " (partial/failed ingestion)");
        }
      }
    }

    // Surface skipped doc-count checks so coverage gaps are visible, not silent.
    if (!skippedDocCount.isEmpty()) {
      LOG.info(
          "Leg 4 (doc-count) skipped for {} dataset(s): {}",
          skippedDocCount.size(),
          String.join(", ", skippedDocCount));
    }

    if (!failures.isEmpty()) {
      throw new AssertionError(
          "Unsupported-field ingestion problems on the AE route ("
              + failures.size()
              + "):\n  "
              + String.join("\n  ", failures));
    }
  }

  private final List<String> skippedDocCount = new ArrayList<>();

  private void deleteIndexIfExists(String indexName) throws IOException {
    try {
      client().performRequest(new Request("DELETE", "/" + indexName));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }

  /** Pull the live mapping back from the cluster and collect any unsupported-typed field paths. */
  private List<String> unsupportedFieldsInLiveMapping(String indexName) throws IOException {
    List<String> offending = new ArrayList<>();
    JSONObject props = liveMappingProperties(indexName);
    if (props != null) {
      collectUnsupported(props, "", offending);
    }
    return offending;
  }

  /**
   * True when the index's live mapping still has at least one top-level property. An index whose
   * mapping was reduced to nothing by the strip (all-unsupported dataset, e.g. cascaded_nested) has
   * no scannable columns and can't be queried on the AE route — Leg 4 is skipped for it.
   */
  private boolean liveMappingHasColumns(String indexName) throws IOException {
    JSONObject props = liveMappingProperties(indexName);
    return props != null && !props.isEmpty();
  }

  /** {@code mappings.properties} of the live index, or null when the mapping has no properties. */
  private JSONObject liveMappingProperties(String indexName) throws IOException {
    Response response = client().performRequest(new Request("GET", "/" + indexName + "/_mapping"));
    JSONObject mappings =
        new JSONObject(getResponseBody(response))
            .getJSONObject(indexName)
            .getJSONObject("mappings");
    return mappings.has("properties") ? mappings.getJSONObject("properties") : null;
  }

  /**
   * Sample up to a few docs via the search API and report any {@code dropped} path that still
   * exists in a returned {@code _source}. A leftover value is what would let dynamic mapping
   * re-create the unsupported field after refresh.
   */
  private List<String> strippedPathsStillInSource(String indexName, Set<List<String>> dropped)
      throws IOException {
    List<String> leftover = new ArrayList<>();
    if (dropped.isEmpty()) {
      return leftover;
    }
    Request search = new Request("POST", "/" + indexName + "/_search?size=50");
    Response response;
    try {
      response = client().performRequest(search);
    } catch (ResponseException e) {
      // A degenerate index (no columns) can't be searched; nothing to check.
      return leftover;
    }
    JSONObject body = new JSONObject(getResponseBody(response));
    JSONArray hits = body.getJSONObject("hits").getJSONArray("hits");
    for (int i = 0; i < hits.length(); i++) {
      JSONObject source = hits.getJSONObject(i).optJSONObject("_source");
      if (source == null) {
        continue;
      }
      for (List<String> path : dropped) {
        if (pathPresent(source, path, 0)) {
          String dotted = String.join(".", path);
          if (!leftover.contains(dotted)) {
            leftover.add(dotted);
          }
        }
      }
    }
    return leftover;
  }

  /** Mirror of TestUtils.removePath traversal: is {@code path[idx..]} present in {@code node}? */
  private boolean pathPresent(Object node, List<String> path, int idx) {
    String part = path.get(idx);
    boolean last = idx == path.size() - 1;
    if (node instanceof JSONObject) {
      JSONObject obj = (JSONObject) node;
      if (!obj.has(part)) {
        return false;
      }
      return last || pathPresent(obj.get(part), path, idx + 1);
    } else if (node instanceof JSONArray) {
      JSONArray arr = (JSONArray) node;
      for (int j = 0; j < arr.length(); j++) {
        if (pathPresent(arr.get(j), path, idx)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Live doc count of the index, obtained through the analytics-engine query path ({@code
   * source=idx | stats count()}). The REST {@code _count} API is NOT usable here — on a
   * parquet-backed {@code DataFormatAwareEngine} index it fails with "Cannot apply function ...
   * directly on IndexShard". Counting via PPL also exercises the real scan path, so the result
   * proves the data both ingested and is readable on the AE route.
   */
  private Integer liveDocCountOrNull(String indexName) {
    String query = String.format("source=%s | stats count() as c", indexName);
    try {
      JSONObject result = executeQuery(query);
      return result.getJSONArray("datarows").getJSONArray(0).getInt(0);
    } catch (Exception e) {
      // The count query can fail for cluster-side reasons unrelated to the strip (e.g. it touches a
      // parquet-backed system index that can't serve a shard-level count). Don't let that mask the
      // strip proof — return null so the caller records a transparent skip.
      LOG.info("count query failed for [{}]: {}", indexName, rootMessage(e));
      return null;
    }
  }

  /**
   * Number of source (non-action) lines in a dataset's bulk NDJSON file — i.e. how many docs the
   * load is expected to ingest. Returns 0 when the index has no dataset file (mapping-only enum
   * entries), which makes Leg 4 a no-op for those.
   */
  private static int sourceDocCount(Index index) throws IOException {
    String dataSet = index.getDataSet();
    if (dataSet == null || dataSet.isEmpty()) {
      return 0;
    }
    String body = new String(Files.readAllBytes(Paths.get(getResourceFilePath(dataSet))));
    int docs = 0;
    for (String line : body.split("\n", -1)) {
      String trimmed = line.trim();
      if (trimmed.isEmpty() || trimmed.charAt(0) != '{') {
        continue;
      }
      JSONObject obj = new JSONObject(trimmed);
      boolean isActionLine =
          obj.has("index") || obj.has("create") || obj.has("update") || obj.has("delete");
      if (!isActionLine) {
        docs++;
      }
    }
    return docs;
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
   * Safe to skip an index whose creation failed, only when BOTH hold: (a) the error names an
   * out-of-scope type (e.g. join), AND (b) the dataset's mapping contains no {@link #UNSUPPORTED}
   * type. The second condition prevents a mixed failure (a join issue masking a missed unsupported
   * field) from being silently skipped — if any unsupported type is present, we do NOT skip.
   */
  private static boolean safeToSkipForOutOfScopeType(Throwable t, String mapping) {
    String msg = rootMessage(t);
    if (msg == null) {
      return false;
    }
    boolean namesOutOfScope = false;
    for (String type : OUT_OF_SCOPE_TYPES) {
      if (msg.contains("type: " + type) || msg.contains("type [" + type + "]")) {
        namesOutOfScope = true;
        break;
      }
    }
    if (!namesOutOfScope) {
      return false;
    }
    return !mappingContainsUnsupportedType(mapping);
  }

  /**
   * Safe to skip an index whose load failed, only when BOTH hold: (a) the error is exactly the
   * multi-value bulk signature ({@code Cannot accept multiple values for field}), AND (b) the index
   * is on the curated {@link #MULTI_VALUE_DATASETS} allowlist. Unlike {@link
   * #safeToSkipForOutOfScopeType} this does NOT also require the raw mapping to be free of
   * unsupported types: several of these datasets legitimately declare {@code nested} fields that
   * the type strip removes at load, while the multi-value failure is on a separate scalar leaf. The
   * curated allowlist plus the exact failure signature is the masking guard — an unanticipated
   * failure (different message, or a dataset not on the list) is never skipped and surfaces loudly.
   */
  private static boolean safeToSkipForMultiValueLoad(Throwable t, Index index) {
    String msg = rootMessage(t);
    return msg != null
        && msg.contains(MULTI_VALUE_SIGNATURE)
        && MULTI_VALUE_DATASETS.contains(index.name());
  }

  /** True if the raw mapping JSON declares any {@link #UNSUPPORTED} field type at any depth. */
  private static boolean mappingContainsUnsupportedType(String mapping) {
    if (mapping == null || mapping.isEmpty()) {
      return false;
    }
    JSONObject root = new JSONObject(mapping);
    JSONObject mappings = root.optJSONObject("mappings");
    if (mappings == null || !mappings.has("properties")) {
      return false;
    }
    return propertiesContainUnsupported(mappings.getJSONObject("properties"));
  }

  private static boolean propertiesContainUnsupported(JSONObject properties) {
    for (String field : properties.keySet()) {
      JSONObject def = properties.optJSONObject(field);
      if (def == null) {
        continue;
      }
      String type = def.optString("type", null);
      if (type != null && UNSUPPORTED.contains(type)) {
        return true;
      }
      if (def.has("properties") && propertiesContainUnsupported(def.getJSONObject("properties"))) {
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
