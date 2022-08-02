/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/**
 * OpenSearch search response.
 */
@EqualsAndHashCode
@ToString
public class OpenSearchResponse implements Iterable<ExprValue> {

  /**
   * Search query result (non-aggregation).
   */
  private final SearchHits hits;

  /**
   * Search aggregation result.
   */
  private final Aggregations aggregations;

  /**
   * ElasticsearchExprValueFactory used to build ExprValue from search result.
   */
  @EqualsAndHashCode.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Constructor of ElasticsearchResponse.
   */
  public OpenSearchResponse(SearchResponse searchResponse,
                            OpenSearchExprValueFactory exprValueFactory) {
    this.hits = searchResponse.getHits();
    this.aggregations = searchResponse.getAggregations();
    this.exprValueFactory = exprValueFactory;
  }

  /**
   * Constructor of ElasticsearchResponse with SearchHits.
   */
  public OpenSearchResponse(SearchHits hits, OpenSearchExprValueFactory exprValueFactory) {
    this.hits = hits;
    this.aggregations = null;
    this.exprValueFactory = exprValueFactory;
  }

  /**
   * Is response empty. As OpenSearch doc says, "Each call to the scroll API returns the next batch
   * of results until there are no more results left to return, ie the hits array is empty."
   *
   * @return true for empty
   */
  public boolean isEmpty() {
    return (hits.getHits() == null) || (hits.getHits().length == 0) && aggregations == null;
  }

  public boolean isAggregationResponse() {
    return aggregations != null;
  }

  /**
   * Make response iterable without need to return internal data structure explicitly.
   *
   * @return search hit iterator
   */
  public Iterator<ExprValue> iterator() {
    if (isAggregationResponse()) {
      return exprValueFactory.getParser().parse(aggregations).stream().map(entry -> {
        ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<String, Object> value : entry.entrySet()) {
          builder.put(value.getKey(), exprValueFactory.construct(value.getKey(), value.getValue()));
        }
        return (ExprValue) ExprTupleValue.fromExprValueMap(builder.build());
      }).iterator();
    } else {
      return Arrays.stream(hits.getHits())
          .map(hit -> {
            ExprValue docData = exprValueFactory.construct(hit.getSourceAsString());
            if (hit.getHighlightFields().isEmpty()) {
              return docData;
            } else {
              ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
              builder.putAll(docData.tupleValue());
              var hlBuilder = ImmutableMap.<String, ExprValue>builder();
              for (var es : hit.getHighlightFields().entrySet()) {
                hlBuilder.put(es.getKey(), ExprValueUtils.stringValue(es.getValue().toString()));
              }
              builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));
              return ExprTupleValue.fromExprValueMap(builder.build());
            }
          }).iterator();
    }
  }
}
