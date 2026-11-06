/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response.agg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import lombok.Value;
import org.apache.lucene.search.TotalHits;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.metrics.TopHits;
import org.opensearch.sql.common.error.ErrorCode;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.error.ResourceLimitExceededException;

/**
 * {@link TopHits} metric parser for ARRAY_AGG aggregations. Every hit contributes exactly one
 * element, in hit order, so duplicates and NULLs are kept like the in-memory ARRAY_AGG does.
 */
@Value
public class ArrayAggParser implements MetricParser {

  String name;

  /**
   * Path of the collected field in {@code _source}, which the in-memory scan reads too, or null
   * when the hit carries the value as its only document field (script or alias fields).
   */
  @Nullable String sourcePath;

  @Override
  public List<Map<String, Object>> parse(Aggregation agg) {
    TopHits topHits = (TopHits) agg;
    SearchHit[] hits = topHits.getHits().getHits();
    TotalHits totalHits = topHits.getHits().getTotalHits();
    if (totalHits != null && totalHits.value() > hits.length) {
      // top_hits returns at most `size` hits per bucket. A truncated array would silently differ
      // from what the in-memory ARRAY_AGG returns.
      throw ErrorReport.wrap(
              new ResourceLimitExceededException(
                  String.format(
                      "Cannot collect all %d values of a group into [%s], only %d were returned.",
                      totalHits.value(), name, hits.length)))
          .code(ErrorCode.RESOURCE_LIMIT_EXCEEDED)
          .details(
              String.format(
                  "The aggregation was pushed down as a top_hits of %d hits per group. Increase"
                      + " both [plugins.query.buckets] and the index setting"
                      + " [index.max_inner_result_window] to at least %d, or disable the"
                      + " aggregation pushdown.",
                  hits.length, totalHits.value()))
          .build();
    }
    // ARRAY_AGG of a group whose rows are all filtered out is NULL, not an empty array.
    List<Object> values = null;
    if (hits.length > 0) {
      values = new ArrayList<>(hits.length);
      for (SearchHit hit : hits) {
        values.add(sourcePath == null ? documentFieldValue(hit) : sourceValue(hit));
      }
    }
    return Collections.singletonList(
        new HashMap<>(Collections.singletonMap(agg.getName(), values)));
  }

  /** The first value of the field; a multi-valued field is not supported as an element. */
  private Object sourceValue(SearchHit hit) {
    Map<String, Object> source = hit.getSourceAsMap();
    Object value = source == null ? null : XContentMapValues.extractValue(sourcePath, source);
    if (value instanceof List<?> list) {
      return list.isEmpty() ? null : list.getFirst();
    }
    return value;
  }

  private static Object documentFieldValue(SearchHit hit) {
    // The only document field is the fetched one; getFields() would add meta fields like _routing.
    return hit.getDocumentFields().values().stream()
        .findFirst()
        .map(DocumentField::getValue)
        .orElse(null);
  }
}
