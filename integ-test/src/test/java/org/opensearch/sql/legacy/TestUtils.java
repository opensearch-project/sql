/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static com.google.common.base.Strings.isNullOrEmpty;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.transport.client.Client;

public class TestUtils {

  private static final String MAPPING_FILE_PATH = "src/test/resources/indexDefinitions/";

  /**
   * Forwarding alias for {@link AnalyticsIndexConfig#ENABLED_PROP}. Kept for any external callers
   * that referenced the old constant location.
   */
  public static final String ANALYTICS_PARQUET_INDICES_PROP = AnalyticsIndexConfig.ENABLED_PROP;

  /**
   * Centralizes every analytics-engine-only test-index knob in one place so the parquet-backed
   * settings, the bulk-load refresh strategy, and any future analytics-specific config don't drift
   * across separate helpers (the spread-out-conditional pattern called out in review, borrowed from
   * the OS-Spark repo's AOSS-conditional configs that ended up scattered across its codebase).
   *
   * <p>When {@link #ENABLED_PROP} is unset (default), every method here is a no-op or returns the
   * standard non-parquet value, so normal CI sees zero behavior change.
   */
  public static final class AnalyticsIndexConfig {

    /**
     * System property that makes every test-created index parquet-backed (composite primary data
     * format = parquet) with a single shard. When set, {@link
     * RestUnifiedQueryAction#isAnalyticsIndex} (which routes based on {@code
     * index.pluggable.dataformat.enabled} / {@code index.pluggable.dataformat=composite}, see
     * #5432) returns {@code true} for every test-created index — exercising the analytics-engine
     * route end-to-end without per-test rewiring.
     */
    public static final String ENABLED_PROP = "tests.analytics.parquet_indices";

    /**
     * System property overriding the number of primary shards for analytics-backed test indices.
     * Defaults to 1 (single-shard). Set to e.g. "3" for multi-shard coverage runs.
     */
    public static final String NUM_SHARDS_PROP = "tests.analytics.num_shards";

    public static boolean isEnabled() {
      return Boolean.parseBoolean(System.getProperty(ENABLED_PROP, "false"));
    }

    public static int getNumShards() {
      return Integer.parseInt(System.getProperty(NUM_SHARDS_PROP, "1"));
    }

    // Composite-store format values shared by the index-level and cluster-level settings below.
    private static final String DATAFORMAT_COMPOSITE = "composite";
    private static final String PRIMARY_FORMAT_PARQUET = "parquet";
    private static final String SECONDARY_FORMAT_LUCENE = "lucene";

    /**
     * Inject the parquet-backed composite-store index settings into {@code jsonObject}. No-op when
     * the config is disabled; idempotent — safe on any index-creation JSON shape.
     */
    static void applyIndexCreationSettings(JSONObject jsonObject) {
      if (!isEnabled()) {
        return;
      }
      JSONObject settings =
          jsonObject.has("settings") ? jsonObject.getJSONObject("settings") : new JSONObject();
      JSONObject indexSettings =
          settings.has("index") ? settings.getJSONObject("index") : new JSONObject();
      indexSettings.put("number_of_shards", getNumShards());
      indexSettings.put("pluggable.dataformat.enabled", true);
      indexSettings.put("pluggable.dataformat", DATAFORMAT_COMPOSITE);
      indexSettings.put("composite.primary_data_format", PRIMARY_FORMAT_PARQUET);
      indexSettings.put(
          "composite.secondary_data_formats", new JSONArray().put(SECONDARY_FORMAT_LUCENE));
      settings.put("index", indexSettings);
      jsonObject.put("settings", settings);
    }

    /**
     * Set the composite-store defaults at the cluster level so even indices auto-created by a raw
     * document {@code PUT} (which bypass {@link #applyIndexCreationSettings}) are parquet-backed.
     * Otherwise such an index inherits only the composite value — so it routes to the analytics
     * engine — but not the {@code .enabled} flag, leaving it stored as a plain-Lucene {@code
     * EngineBackedIndexer} that fails at query time. No-op when disabled; idempotent.
     */
    public static void applyClusterSettings(RestClient client) {
      if (!isEnabled()) {
        return;
      }
      JSONObject persistent =
          new JSONObject()
              .put("cluster.pluggable.dataformat.enabled", true)
              .put("cluster.pluggable.dataformat", DATAFORMAT_COMPOSITE)
              .put("cluster.composite.primary_data_format", PRIMARY_FORMAT_PARQUET)
              .put(
                  "cluster.composite.secondary_data_formats",
                  new JSONArray().put(SECONDARY_FORMAT_LUCENE));
      Request request = new Request("PUT", "/_cluster/settings");
      request.setJsonEntity(new JSONObject().put("persistent", persistent).toString());
      performRequest(client, request);
    }

    /**
     * Returns the {@code _bulk} refresh query string for the current index type. Parquet-backed
     * indices in the analytics-backend-lucene composite engine don't yet implement {@code
     * LuceneCommitter.getSafeCommitInfo} (UnsupportedOperationException "TODO:: with index
     * deleter"), which hangs {@code refresh=wait_for} until the test framework request timeout
     * (~60s). Force-refresh sidesteps the safe-commit-info path while still making the bulk-loaded
     * docs immediately searchable. Drop this branch once {@code LuceneCommitter.getSafeCommitInfo}
     * is implemented.
     */
    static String bulkLoadRefreshParam() {
      return isEnabled() ? "refresh=true" : "refresh=wait_for&wait_for_active_shards=all";
    }

    /**
     * Field types the analytics-engine (DataFusion) backend cannot read, and which therefore must
     * be removed from a test index's mapping (and the matching key from each bulk doc) before the
     * index is created/loaded on the analytics-engine route. Seeding a dataset that happens to use
     * one of them would otherwise fail ingestion and surface as an unrelated {@code expected:<1>
     * but was:<0>} downstream — so we strip uniformly at load time across every dataset, no
     * per-index variant files to author or keep in sync.
     *
     * <p><b>This set is the single source of truth.</b> The strip triggers purely on a mapping's
     * field <em>types</em> (not on which IT is running), so any IT — existing or newly added — that
     * loads one of these datasets through {@link SQLIntegTestCase#loadIndex} is handled out of the
     * box. {@code AnalyticsUnsupportedFieldStripVerifyIT} imports this same constant and proves no
     * listed type survives in any live mapping, so the list cannot silently drift.
     *
     * <p><b>This set is the always-strip core.</b> {@code binary} is handled conditionally and is
     * NOT in this set — see {@link #isUnsupportedForAeStore}.
     *
     * <p><b>What is and isn't here, and why:</b>
     *
     * <ul>
     *   <li>{@code nested} — entire subtree dropped (the engine has no nested-document support).
     *   <li>{@code geo_point}, {@code geo_shape} — fall through the engine's type whitelist ({@code
     *       OpenSearchSchemaBuilder.mapFieldType} default → null), so the column can't scan.
     *   <li>{@code alias} — indirection the scan-row-type builder doesn't resolve.
     *   <li><b>{@code binary} is conditional, not in this set.</b> The engine's read path maps
     *       {@code binary → VARBINARY} (same as {@code ip}), so a binary column scans fine — but
     *       the parquet/composite <em>store</em> rejects creating a {@code binary} field that lacks
     *       {@code store: true} ("Unable to derive source for [X] with store disabled", verified on
     *       the AE cluster). So we strip a {@code binary} field only when it is NOT {@code store:
     *       true}; a {@code store: true} binary field is kept and ingests/scans normally. Adding
     *       {@code binary} to this unconditional set would over-strip the supported case.
     * </ul>
     */
    public static final Set<String> UNSUPPORTED_FIELD_TYPES =
        Set.of("nested", "geo_point", "geo_shape", "alias");

    /**
     * Whether a field definition cannot be created/scanned on the analytics-engine route and so
     * must be stripped. True for any {@link #UNSUPPORTED_FIELD_TYPES} type, and additionally for a
     * {@code binary} field that is not {@code store: true} — the parquet store can't derive {@code
     * _source} for a store-disabled binary field at mapping time.
     */
    private static boolean isUnsupportedForAeStore(JSONObject def) {
      String type = def.optString("type", null);
      if (type == null) {
        return false;
      }
      if (UNSUPPORTED_FIELD_TYPES.contains(type)) {
        return true;
      }
      return "binary".equals(type) && !def.optBoolean("store", false);
    }

    /**
     * Remove every {@link #UNSUPPORTED_FIELD_TYPES} property (recursively, including object
     * sub-properties) from an index-creation JSON's {@code mappings.properties}, and report the
     * <em>exact dotted paths</em> that were dropped so the matching values can be stripped from
     * bulk data. No-op when disabled. Mutates {@code jsonObject} in place.
     *
     * <p>Paths are returned as ordered part lists, e.g. {@code ["location", "point"]} for a {@code
     * geo_point} sub-field of an object {@code location}, so the bulk-source strip can remove the
     * exact nested value while preserving unaffected siblings ({@code location.city} etc.). A
     * top-level unsupported field comes back as a single-element list, e.g. {@code
     * ["nested_value"]}.
     *
     * @return the dropped field paths (empty when disabled or nothing matched)
     */
    static Set<List<String>> stripUnsupportedMappingFields(JSONObject jsonObject) {
      if (!isEnabled() || !jsonObject.has("mappings")) {
        return Set.of();
      }
      JSONObject mappings = jsonObject.getJSONObject("mappings");
      if (!mappings.has("properties")) {
        return Set.of();
      }
      Set<List<String>> dropped = new HashSet<>();
      collectAndRemoveUnsupported(mappings.getJSONObject("properties"), new ArrayList<>(), dropped);
      return dropped;
    }

    /**
     * Walk a {@code properties} block, removing every property whose {@code type} is unsupported
     * (recording its full path) and recursing into object sub-{@code properties} for the rest.
     */
    private static void collectAndRemoveUnsupported(
        JSONObject properties, List<String> prefix, Set<List<String>> dropped) {
      for (String field : properties.keySet().toArray(new String[0])) {
        JSONObject def = properties.optJSONObject(field);
        if (def == null) {
          continue;
        }
        List<String> path = new ArrayList<>(prefix);
        path.add(field);
        if (isUnsupportedForAeStore(def)) {
          properties.remove(field);
          dropped.add(List.copyOf(path));
        } else if (def.has("properties")) {
          collectAndRemoveUnsupported(def.getJSONObject("properties"), path, dropped);
        }
      }
    }

    /**
     * Strip the given dropped <em>paths</em> from every source document of a bulk NDJSON payload.
     * Bulk format alternates an action line ({@code {"index":{...}}}) with a source line; only
     * source lines (those without a bulk action key) are rewritten. No-op when disabled or {@code
     * droppedPaths} is empty.
     *
     * <p>Each path is removed recursively: it descends through nested objects <em>and arrays of
     * objects</em> (so a {@code nested}/object array has the field stripped from every element),
     * leaving unaffected siblings intact. A source line is re-serialized <em>only when removing a
     * path actually changed it</em>; every other line (action lines and docs that never had the
     * dropped path) is appended byte-for-byte unchanged, so untouched docs match the fixture
     * exactly.
     */
    static String stripBulkFields(String bulkBody, Set<List<String>> droppedPaths) {
      if (!isEnabled() || droppedPaths.isEmpty()) {
        return bulkBody;
      }
      String[] lines = bulkBody.split("\n", -1);
      StringBuilder out = new StringBuilder(bulkBody.length());
      for (int i = 0; i < lines.length; i++) {
        String line = lines[i];
        String trimmed = line.trim();
        if (!trimmed.isEmpty() && trimmed.charAt(0) == '{') {
          JSONObject doc = new JSONObject(trimmed);
          boolean isActionLine =
              doc.has("index") || doc.has("create") || doc.has("update") || doc.has("delete");
          if (!isActionLine) {
            boolean removedAny = false;
            for (List<String> path : droppedPaths) {
              removedAny |= removePath(doc, path, 0);
            }
            // Only rewrite the line if we actually removed something; otherwise leave it verbatim
            // so untouched docs stay byte-for-byte identical to the fixture.
            if (removedAny) {
              line = doc.toString();
            }
          }
        }
        out.append(line);
        if (i < lines.length - 1) {
          out.append('\n');
        }
      }
      return out.toString();
    }

    /**
     * Remove {@code path[idx..]} from {@code node}, descending through objects and arrays of
     * objects. Returns true if anything was removed. At the last path part the key is deleted from
     * the object; otherwise we recurse into the child object (or each object element of a child
     * array).
     */
    private static boolean removePath(Object node, List<String> path, int idx) {
      String part = path.get(idx);
      boolean last = idx == path.size() - 1;
      boolean removed = false;
      if (node instanceof JSONObject) {
        JSONObject obj = (JSONObject) node;
        if (!obj.has(part)) {
          return false;
        }
        if (last) {
          obj.remove(part);
          return true;
        }
        removed = removePath(obj.get(part), path, idx + 1);
      } else if (node instanceof JSONArray) {
        // The mapping path can sit under an array (e.g. a nested/object array); strip from each
        // object element, ignoring non-object elements.
        JSONArray arr = (JSONArray) node;
        for (int j = 0; j < arr.length(); j++) {
          removed |= removePath(arr.get(j), path, idx);
        }
      }
      return removed;
    }

    private AnalyticsIndexConfig() {}
  }

  /**
   * Create test index by REST client.
   *
   * @param client client connection
   * @param indexName test index name
   * @param mapping test index mapping or null if no predefined mapping
   */
  public static void createIndexByRestClient(RestClient client, String indexName, String mapping) {
    Request request = new Request("PUT", "/" + indexName);
    JSONObject jsonObject = isNullOrEmpty(mapping) ? new JSONObject() : new JSONObject(mapping);
    setZeroReplicas(jsonObject);
    AnalyticsIndexConfig.applyIndexCreationSettings(jsonObject);
    // On the analytics-engine route, drop fields of types DataFusion can't read so the index can
    // still be created from datasets that happen to use them. No-op otherwise.
    AnalyticsIndexConfig.stripUnsupportedMappingFields(jsonObject);
    request.setJsonEntity(jsonObject.toString());
    performRequest(client, request);
  }

  /**
   * Exact dropped field <em>paths</em> the analytics-engine route would remove from {@code mapping}
   * (e.g. {@code ["location", "point"]} for a nested {@code geo_point}). Used by {@link
   * #loadDataByRestClient(RestClient, String, String, java.util.Set)} so bulk docs drop the same
   * paths the mapping dropped. Returns an empty set when AE is disabled or {@code mapping} is
   * empty.
   */
  public static Set<List<String>> analyticsDroppedFields(String mapping) {
    if (isNullOrEmpty(mapping)) {
      return Set.of();
    }
    return AnalyticsIndexConfig.stripUnsupportedMappingFields(new JSONObject(mapping));
  }

  /**
   * Sets number_of_replicas to 0 in the index settings. This makes multi-node behavior consistent
   * (<a href="https://github.com/opensearch-project/sql/issues/4261">4261</a>) and prevents tests
   * from hanging on single-node clusters when using wait_for_active_shards=all.
   *
   * @param jsonObject the index creation JSON object to modify
   */
  private static void setZeroReplicas(JSONObject jsonObject) {
    JSONObject settings =
        jsonObject.has("settings") ? jsonObject.getJSONObject("settings") : new JSONObject();
    JSONObject indexSettings =
        settings.has("index") ? settings.getJSONObject("index") : new JSONObject();
    indexSettings.put("number_of_replicas", 0);
    settings.put("index", indexSettings);
    jsonObject.put("settings", settings);
  }

  /**
   * https://github.com/elastic/elasticsearch/pull/49959<br>
   * Deprecate creation of dot-prefixed index names except for hidden and system indices. Create
   * hidden index by REST client.
   *
   * @param client client connection
   * @param indexName test index name
   * @param mapping test index mapping or null if no predefined mapping
   */
  public static void createHiddenIndexByRestClient(
      RestClient client, String indexName, String mapping) {
    Request request = new Request("PUT", "/" + indexName);
    JSONObject jsonObject = isNullOrEmpty(mapping) ? new JSONObject() : new JSONObject(mapping);
    jsonObject.put("settings", new JSONObject("{\"index\":{\"hidden\":true}}"));
    request.setJsonEntity(jsonObject.toString());

    performRequest(client, request);
  }

  /**
   * Check if index already exists by OpenSearch index exists API which returns: 200 - specified
   * indices or aliases exist 404 - one or more indices specified or aliases do not exist
   *
   * @param client client connection
   * @param indexName index name
   * @return true for index exist
   */
  public static boolean isIndexExist(RestClient client, String indexName) {
    try {
      Response response = client.performRequest(new Request("HEAD", "/" + indexName));
      return (response.getStatusLine().getStatusCode() == 200);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform request", e);
    }
  }

  /**
   * Load test data set by REST client.
   *
   * @param client client connection
   * @param indexName index name
   * @param dataSetFilePath file path of test data set
   * @throws IOException
   */
  public static void loadDataByRestClient(
      RestClient client, String indexName, String dataSetFilePath) throws IOException {
    loadDataByRestClient(client, indexName, dataSetFilePath, Set.of());
  }

  /**
   * Same as {@link #loadDataByRestClient(RestClient, String, String)} but strips {@code
   * droppedPaths} (the exact field paths removed from the mapping on the analytics-engine route)
   * from every bulk source doc, so the index mapping and the data agree. When AE is disabled or
   * {@code droppedPaths} is empty this is byte-for-byte identical to the 3-arg form.
   */
  public static void loadDataByRestClient(
      RestClient client, String indexName, String dataSetFilePath, Set<List<String>> droppedPaths)
      throws IOException {
    Path path = Paths.get(getResourceFilePath(dataSetFilePath));
    String body = new String(Files.readAllBytes(path));
    body = AnalyticsIndexConfig.stripBulkFields(body, droppedPaths);
    Request request =
        new Request(
            "POST", "/" + indexName + "/_bulk?" + AnalyticsIndexConfig.bulkLoadRefreshParam());
    request.setJsonEntity(body);
    Response response = performRequest(client, request);
    failIfBulkHadItemErrors(indexName, response);
  }

  /**
   * A {@code _bulk} call can return HTTP 200 while individual items failed ({@code "errors":true}).
   * That silent partial ingestion is exactly the failure mode that surfaces later as an unrelated
   * row-count/assertion error in some downstream IT, so fail loudly here — naming the index and the
   * first few item errors — for ALL test bulk loads, not just the analytics-engine route.
   */
  private static void failIfBulkHadItemErrors(String indexName, Response response)
      throws IOException {
    JSONObject body = new JSONObject(getResponseBody(response));
    if (!body.optBoolean("errors", false)) {
      return;
    }
    JSONArray items = body.optJSONArray("items");
    List<String> firstErrors = new ArrayList<>();
    if (items != null) {
      for (int i = 0; i < items.length() && firstErrors.size() < 5; i++) {
        JSONObject action = items.getJSONObject(i);
        // each item is { "<op>": { ..., "error": {...} } } where <op> is index/create/update/delete
        for (String op : action.keySet()) {
          JSONObject result = action.optJSONObject(op);
          if (result != null && result.has("error")) {
            firstErrors.add("doc#" + i + ": " + result.get("error"));
          }
        }
      }
    }
    throw new IllegalStateException(
        "Bulk load into ["
            + indexName
            + "] had item failures (errors=true). First failures:\n  "
            + String.join("\n  ", firstErrors));
  }

  /**
   * Return how many docs in the index
   *
   * @param client client connection
   * @param indexName index name
   * @return doc count of the index
   * @throws IOException
   */
  public static int getDocCount(RestClient client, String indexName) throws IOException {
    Request request = new Request("GET", "/" + indexName + "/_count");
    Response response = performRequest(client, request);
    JSONObject jsonObject = new JSONObject(getResponseBody(response));
    return jsonObject.getInt("count");
  }

  /**
   * Perform a request by REST client.
   *
   * @param client client connection
   * @param request request object
   */
  public static Response performRequest(RestClient client, Request request) {
    try {
      Response response = client.performRequest(request);
      int status = response.getStatusLine().getStatusCode();
      if (status >= 400) {
        throw new IllegalStateException("Failed to perform request. Error code: " + status);
      }
      return response;
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform request", e);
    }
  }

  public static String getAccountIndexMapping() {
    String mappingFile = "account_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getAccountExtendedIndexMapping() {
    String mappingFile = "account_extended_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getPhraseIndexMapping() {
    String mappingFile = "phrase_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogIndexMapping() {
    String mappingFile = "dog_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogs2IndexMapping() {
    String mappingFile = "dog2_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDogs3IndexMapping() {
    String mappingFile = "dog3_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getPeople2IndexMapping() {
    String mappingFile = "people2_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGameOfThronesIndexMapping() {
    String mappingFile = "game_of_thrones_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  // System

  public static String getOdbcIndexMapping() {
    String mappingFile = "odbc_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getLocationIndexMapping() {
    String mappingFile = "location_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getLocationsTypeConflictIndexMapping() {
    String mappingFile = "locations_type_conflict_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getEmployeeNestedTypeIndexMapping() {
    String mappingFile = "employee_nested_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getNestedTypeIndexMapping() {
    String mappingFile = "nested_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getJoinTypeIndexMapping() {
    String mappingFile = "join_type_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getUnexpandedObjectIndexMapping() {
    String mappingFile = "unexpanded_object_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getBankIndexMapping() {
    String mappingFile = "bank_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getBankExtendedIndexMapping() {
    String mappingFile = "bank_extended_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGeoIpIndexMapping() {
    String mappingFile = "geoip_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getBankWithNullValuesIndexMapping() {
    String mappingFile = "bank_with_null_values_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getStringIndexMapping() {
    String mappingFile = "string_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getOrderIndexMapping() {
    String mappingFile = "order_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getWeblogsIndexMapping() {
    String mappingFile = "weblogs_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateIndexMapping() {
    String mappingFile = "date_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateTimeIndexMapping() {
    String mappingFile = "date_time_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateTimeNestedIndexMapping() {
    String mappingFile = "date_time_nested_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getNestedSimpleIndexMapping() {
    String mappingFile = "nested_simple_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDeepNestedIndexMapping() {
    String mappingFile = "deep_nested_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDataTypeNumericIndexMapping() {
    String mappingFile = "datatypes_numeric_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDataTypeNonnumericIndexMapping() {
    String mappingFile = "datatypes_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDateTimeSimpleIndexMapping() {
    String mappingFile = "datetime_simple_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGeopointIndexMapping() {
    String mappingFile = "geopoint_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getComplexGeoIndexMapping() {
    String mappingFile = "complex_geo_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getJsonTestIndexMapping() {
    String mappingFile = "json_test_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getAliasIndexMapping() {
    String mappingFile = "alias_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getStateCountryIndexMapping() {
    String mappingFile = "state_country_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getOccupationIndexMapping() {
    String mappingFile = "occupation_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getHobbiesIndexMapping() {
    String mappingFile = "hobbies_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getWorkerIndexMapping() {
    String mappingFile = "worker_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getWorkInformationIndexMapping() {
    String mappingFile = "work_information_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGraphEmployeesIndexMapping() {
    String mappingFile = "graph_employees_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGraphTravelersIndexMapping() {
    String mappingFile = "graph_travelers_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getGraphAirportsIndexMapping() {
    String mappingFile = "graph_airports_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getDuplicationNullableIndexMapping() {
    String mappingFile = "duplication_nullable_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getArrayIndexMapping() {
    String mappingFile = "array_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getHdfsLogsIndexMapping() {
    String mappingFile = "hdfs_logs_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getLogsIndexMapping() {
    String mappingFile = "logs_index_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static String getOtelLogsIndexMapping() {
    String mappingFile = "otellogs_mapping.json";
    return getMappingFile(mappingFile);
  }

  public static void loadBulk(Client client, String jsonPath, String defaultIndex)
      throws Exception {
    System.out.println(String.format("Loading file %s into opensearch cluster", jsonPath));
    String absJsonPath = getResourceFilePath(jsonPath);

    BulkRequest bulkRequest = new BulkRequest();
    try (final InputStream stream = new FileInputStream(absJsonPath);
        final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        final BufferedReader br = new BufferedReader(streamReader)) {

      while (true) {

        String actionLine = br.readLine();
        if (actionLine == null || actionLine.trim().isEmpty()) {
          break;
        }
        String sourceLine = br.readLine();
        JSONObject actionJson = new JSONObject(actionLine);

        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(defaultIndex);
        if (actionJson.getJSONObject("index").has("_id")) {
          String docId = actionJson.getJSONObject("index").getString("_id");
          indexRequest.id(docId);
        }
        if (actionJson.getJSONObject("index").has("_routing")) {
          String routing = actionJson.getJSONObject("index").getString("_routing");
          indexRequest.routing(routing);
        }
        indexRequest.source(sourceLine, XContentType.JSON);
        bulkRequest.add(indexRequest);
      }
    }

    BulkResponse bulkResponse = client.bulk(bulkRequest).actionGet();

    if (bulkResponse.hasFailures()) {
      throw new Exception(
          "Failed to load test data into index "
              + defaultIndex
              + ", "
              + bulkResponse.buildFailureMessage());
    }
    System.out.println(bulkResponse.getItems().length + " documents loaded.");
    // ensure the documents are searchable
    client.admin().indices().prepareRefresh(defaultIndex).execute().actionGet();
  }

  public static String getResourceFilePath(String relPath) {
    String projectRoot = System.getProperty("project.root", null);
    if (projectRoot == null) {
      return new File(relPath).getAbsolutePath();
    } else {
      return new File(projectRoot + "/" + relPath).getAbsolutePath();
    }
  }

  public static String getResponseBody(Response response) throws IOException {

    return getResponseBody(response, false);
  }

  public static String getResponseBody(Response response, boolean retainNewLines)
      throws IOException {
    final StringBuilder sb = new StringBuilder();

    try (final InputStream is = response.getEntity().getContent();
        final BufferedReader br =
            new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {

      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line);
        if (retainNewLines) {
          sb.append(String.format(Locale.ROOT, "%n"));
        }
      }
    }
    return sb.toString();
  }

  public static String fileToString(
      final String filePathFromProjectRoot, final boolean removeNewLines) throws IOException {

    final String absolutePath = getResourceFilePath(filePathFromProjectRoot);

    try (final InputStream stream = new FileInputStream(absolutePath);
        final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        final BufferedReader br = new BufferedReader(streamReader)) {

      final StringBuilder stringBuilder = new StringBuilder();
      String line = br.readLine();

      while (line != null) {

        stringBuilder.append(line);
        if (!removeNewLines) {
          stringBuilder.append(String.format(Locale.ROOT, "%n"));
        }
        line = br.readLine();
      }

      return stringBuilder.toString();
    }
  }

  /**
   * Builds all permutations of the given list of Strings
   *
   * @param items list of strings to permute
   * @return list of permutations
   */
  public static List<List<String>> getPermutations(final List<String> items) {

    if (items.size() > 5) {
      throw new IllegalArgumentException("Inefficient test, please refactor");
    }

    final List<List<String>> result = new LinkedList<>();

    if (items.isEmpty() || 1 == items.size()) {

      final List<String> onlyElement = new ArrayList<>();
      if (1 == items.size()) {
        onlyElement.add(items.get(0));
      }
      result.add(onlyElement);
      return result;
    }

    for (int i = 0; i < items.size(); ++i) {

      final List<String> smallerSet = new ArrayList<>();

      if (i != 0) {
        smallerSet.addAll(items.subList(0, i));
      }
      if (i != items.size() - 1) {
        smallerSet.addAll(items.subList(i + 1, items.size()));
      }

      final String currentItem = items.get(i);
      result.addAll(
          getPermutations(smallerSet).stream()
              .map(
                  smallerSetPermutation -> {
                    final List<String> permutation = new ArrayList<>();
                    permutation.add(currentItem);
                    permutation.addAll(smallerSetPermutation);
                    return permutation;
                  })
              .collect(Collectors.toCollection(LinkedList::new)));
    }

    return result;
  }

  public static String getMappingFile(String fileName) {
    try {
      return fileToString(MAPPING_FILE_PATH + fileName, false);
    } catch (IOException e) {
      return null;
    }
  }

  public static String getTpchMappingFile(String fileName) {
    try {
      return TestUtils.fileToString("src/test/resources/tpch/mappings/" + fileName, false);
    } catch (IOException e) {
      return null;
    }
  }

  public static String getBig5MappingFile(String fileName) {
    try {
      return TestUtils.fileToString("src/test/resources/big5/mappings/" + fileName, false);
    } catch (IOException e) {
      return null;
    }
  }

  public static String getClickBenchMappingFile(String fileName) {
    try {
      return TestUtils.fileToString("src/test/resources/clickbench/mappings/" + fileName, false);
    } catch (IOException e) {
      return null;
    }
  }
}
