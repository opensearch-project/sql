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
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScan;
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
  private final FilterType filterType; // null means default (POST)

  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options,
      FilterType filterType) {
    super(client, settings, indexName);
    this.field = field;
    this.vector = vector;
    this.options = options;
    this.filterType = filterType;
  }

  /** Default constructor — preserves existing call sites; uses no explicit filter type. */
  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options) {
    this(client, settings, indexName, field, vector, options, null);
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    final TimeValue cursorKeepAlive =
        getSettings().getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    var requestBuilder = createRequestBuilder();

    // Callback for efficient filtering: serialize WHERE QueryBuilder to JSON,
    // rebuild knn query with filter embedded. JSON handling stays in this class.
    Function<QueryBuilder, QueryBuilder> rebuildWithFilter =
        whereQuery -> new WrapperQueryBuilder(buildKnnQueryJson(whereQuery.toString()));

    boolean filterTypeExplicit = filterType != null;
    FilterType effectiveFilterType = filterType != null ? filterType : FilterType.POST;

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
        rb ->
            new OpenSearchIndexScan(
                getClient(),
                rb.getMaxResponseSize(),
                rb.build(getIndexName(), cursorKeepAlive, getClient(), getFieldTypes().isEmpty()));
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
