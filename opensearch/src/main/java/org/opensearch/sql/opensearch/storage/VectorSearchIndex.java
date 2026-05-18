/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.Map;
import java.util.function.Function;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.storage.capability.KnnPluginCapability;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
import org.opensearch.sql.opensearch.storage.scan.VectorSearchIndexScan;
import org.opensearch.sql.opensearch.storage.scan.VectorSearchIndexScanBuilder;
import org.opensearch.sql.opensearch.storage.scan.VectorSearchQueryBuilder;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Vector-search-aware OpenSearch index. Seeds the scan with a knn query and enables score tracking.
 */
public class VectorSearchIndex extends OpenSearchIndex {

  private final String field;
  private final float[] vector;
  private final Map<String, String> options;
  private final FilterType filterType; // null means default (EFFICIENT)
  // Nullable for back-compat with existing tests and the non-vector-search constructor. When
  // present, the scan defers a lazy k-NN plugin probe to open() so execution fails fast with a
  // clear SQL error if the plugin is missing.
  private final KnnPluginCapability knnCapability;

  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options,
      FilterType filterType,
      KnnPluginCapability knnCapability) {
    super(client, settings, indexName);
    this.field = field;
    this.vector = vector;
    this.options = options;
    this.filterType = filterType;
    this.knnCapability = knnCapability;
  }

  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options,
      FilterType filterType) {
    this(client, settings, indexName, field, vector, options, filterType, null);
  }

  /**
   * Default constructor — preserves existing call sites; uses no explicit filter type, so the scan
   * falls back to the default placement ({@link FilterType#EFFICIENT}).
   */
  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options) {
    this(client, settings, indexName, field, vector, options, null, null);
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    // _score is not blocked at mapping time, so a user field named _score would collide with the
    // synthetic v._score column on the response tuple and fail with an opaque duplicate-key error.
    // Reject here so the user sees a clear SQL error (and _explain surfaces the problem without a
    // k-NN request).
    if (getFieldTypes().containsKey(METADATA_FIELD_SCORE)) {
      throw new IllegalArgumentException(
          String.format(
              "Index '%s' defines a user field named '_score' that collides with the synthetic"
                  + " _score column exposed by vectorSearch(). Rename the field or query the index"
                  + " without vectorSearch().",
              getIndexName()));
    }
    final TimeValue cursorKeepAlive =
        getSettings().getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    var requestBuilder = createRequestBuilder();

    // Callback for efficient filtering: serialize WHERE QueryBuilder to JSON,
    // rebuild knn query with filter embedded. JSON handling stays in this class.
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder(buildKnnQueryJson(whereQuery.toString()));

    boolean filterTypeExplicit = filterType != null;
    FilterType effectiveFilterType = filterType != null ? filterType : FilterType.EFFICIENT;

    var queryBuilder =
        new VectorSearchQueryBuilder(
            requestBuilder,
            buildKnnQuery(),
            options,
            effectiveFilterType,
            filterTypeExplicit,
            rebuildWithFilter);
    requestBuilder.pushDownTrackedScore(true);

    // Default size policy: LIMIT pushdown will further reduce if present.
    if (options.containsKey("k")) {
      // Top-k mode: default size to k so queries without LIMIT return k results.
      requestBuilder.pushDownLimitToRequestTotal(Integer.parseInt(options.get("k")), 0);
    } else {
      // Radial mode (max_distance/min_score): cap at maxResultWindow.
      // Without an explicit cap, radial queries could return unbounded results.
      requestBuilder.pushDownLimitToRequestTotal(getMaxResultWindow(), 0);
    }

    Function<OpenSearchRequestBuilder, OpenSearchIndexScan> createScanOperator =
        rb -> {
          var request =
              rb.build(getIndexName(), cursorKeepAlive, getClient(), getFieldTypes().isEmpty());
          if (knnCapability != null) {
            return new VectorSearchIndexScan(
                getClient(), rb.getMaxResponseSize(), request, knnCapability);
          }
          return new OpenSearchIndexScan(getClient(), rb.getMaxResponseSize(), request);
        };
    return new VectorSearchIndexScanBuilder(queryBuilder, createScanOperator);
  }

  private QueryBuilder buildKnnQuery() {
    return new WrapperQueryBuilder(buildKnnQueryJson());
  }

  // Package-private for testing
  String buildKnnQueryJson() {
    return buildKnnQueryJson(null);
  }

  /**
   * Builds knn query JSON, optionally embedding a filter clause for efficient filtering.
   *
   * @param filterJson serialized filter JSON to embed in knn.field.filter, or null for no filter
   */
  String buildKnnQueryJson(String filterJson) {
    StringBuilder vectorJson = new StringBuilder("[");
    for (int i = 0; i < vector.length; i++) {
      if (i > 0) vectorJson.append(",");
      vectorJson.append(vector[i]);
    }
    vectorJson.append("]");

    StringBuilder optionsJson = new StringBuilder();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      optionsJson.append(",");
      String value = entry.getValue();
      // All P0 option values are canonicalized to numeric strings by validateOptions().
      // The quoted fallback is retained for forward compatibility with future non-numeric options.
      if (isNumeric(value)) {
        optionsJson.append(String.format("\"%s\":%s", entry.getKey(), value));
      } else {
        optionsJson.append(String.format("\"%s\":\"%s\"", entry.getKey(), value));
      }
    }

    String filterClause = "";
    if (filterJson != null) {
      filterClause = String.format(",\"filter\":%s", filterJson);
    }

    return String.format(
        "{\"knn\":{\"%s\":{\"vector\":%s%s%s}}}",
        field, vectorJson.toString(), optionsJson.toString(), filterClause);
  }

  private static boolean isNumeric(String str) {
    try {
      Double.parseDouble(str);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
