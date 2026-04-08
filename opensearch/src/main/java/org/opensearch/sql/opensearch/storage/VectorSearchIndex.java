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
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanBuilder;
import org.opensearch.sql.opensearch.storage.scan.VectorSearchQueryBuilder;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Vector-search-aware OpenSearch index. Seeds the scan with a knn query and enables score tracking.
 */
public class VectorSearchIndex extends OpenSearchIndex {

  private final String field;
  private final float[] vector;
  private final Map<String, String> options;

  public VectorSearchIndex(
      OpenSearchClient client,
      Settings settings,
      String indexName,
      String field,
      float[] vector,
      Map<String, String> options) {
    super(client, settings, indexName);
    this.field = field;
    this.vector = vector;
    this.options = options;
  }

  @Override
  public TableScanBuilder createScanBuilder() {
    final TimeValue cursorKeepAlive =
        getSettings().getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE);
    var requestBuilder = createRequestBuilder();

    // Use VectorSearchQueryBuilder to keep knn in must (scoring) context.
    // WHERE filters will be placed in filter (non-scoring) context.
    var queryBuilder = new VectorSearchQueryBuilder(requestBuilder, buildKnnQuery(), options);
    requestBuilder.pushDownTrackedScore(true);

    // Top-k mode: default size to k so queries without LIMIT return k results
    // instead of falling into the generic large-scan path.
    // LIMIT pushdown will further reduce this if present.
    if (options.containsKey("k")) {
      requestBuilder.pushDownLimitToRequestTotal(Integer.parseInt(options.get("k")), 0);
    }

    Function<OpenSearchRequestBuilder, OpenSearchIndexScan> createScanOperator =
        rb ->
            new OpenSearchIndexScan(
                getClient(),
                rb.getMaxResponseSize(),
                rb.build(getIndexName(), cursorKeepAlive, getClient(), getFieldTypes().isEmpty()));
    return new OpenSearchIndexScanBuilder(queryBuilder, createScanOperator);
  }

  private QueryBuilder buildKnnQuery() {
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

    String knnQueryJson =
        String.format(
            "{\"knn\":{\"%s\":{\"vector\":%s%s}}}",
            field, vectorJson.toString(), optionsJson.toString());
    return new WrapperQueryBuilder(knnQueryJson);
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
