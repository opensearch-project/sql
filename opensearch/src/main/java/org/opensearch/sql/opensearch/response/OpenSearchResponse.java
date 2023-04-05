/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response;

import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_INDEX;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_MAXSCORE;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_SCORE;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_SORT;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
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
   * List of requested include fields.
   */
  private final List<String> includes;

  /**
   * ElasticsearchExprValueFactory used to build ExprValue from search result.
   */
  @EqualsAndHashCode.Exclude
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Constructor of ElasticsearchResponse.
   */
  public OpenSearchResponse(SearchResponse searchResponse,
                            OpenSearchExprValueFactory exprValueFactory,
                            List<String> includes) {
    this.hits = searchResponse.getHits();
    this.aggregations = searchResponse.getAggregations();
    this.exprValueFactory = exprValueFactory;
    this.includes = includes;
  }

  /**
   * Constructor of ElasticsearchResponse with SearchHits.
   */
  public OpenSearchResponse(SearchHits hits,
                            OpenSearchExprValueFactory exprValueFactory,
                            List<String> includes) {
    this.hits = hits;
    this.aggregations = null;
    this.exprValueFactory = exprValueFactory;
    this.includes = includes;
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
      boolean includeId = includes.stream().anyMatch(
          include -> include.equalsIgnoreCase(METADATA_FIELD_ID)
      );
      boolean includeIndex = includes.stream().anyMatch(
          include -> include.equalsIgnoreCase(METADATA_FIELD_INDEX)
      );
      boolean includeScore = includes.stream().anyMatch(
          include -> include.equalsIgnoreCase(METADATA_FIELD_SCORE)
      );
      boolean includeMaxScore = includes.stream().anyMatch(
          include -> include.equalsIgnoreCase(METADATA_FIELD_MAXSCORE)
      );
      boolean includeSort = includes.stream().anyMatch(
          include -> include.equalsIgnoreCase(METADATA_FIELD_SORT)
      );
      ExprFloatValue maxScore = Float.isNaN(hits.getMaxScore())
          ? null : new ExprFloatValue(hits.getMaxScore());
      return Arrays.stream(hits.getHits())
          .map(hit -> {
            String source = hit.getSourceAsString();
            ExprValue docData = exprValueFactory.construct(source);

            ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
            builder.putAll(docData.tupleValue());
            if (includeIndex) {
              builder.put("_index", new ExprStringValue(hit.getIndex()));
            }
            if (includeId) {
              builder.put("_id", new ExprStringValue(hit.getId()));
            }
            if (includeScore && !Float.isNaN(hit.getScore())) {
              builder.put("_score", new ExprFloatValue(hit.getScore()));
            }
            if (includeMaxScore && maxScore != null) {
              builder.put("_maxscore", maxScore);
            }
            if (includeSort) {
              builder.put("_sort", new ExprLongValue(hit.getSeqNo()));
            }

            if (!hit.getHighlightFields().isEmpty()) {
              var hlBuilder = ImmutableMap.<String, ExprValue>builder();
              for (var es : hit.getHighlightFields().entrySet()) {
                hlBuilder.put(es.getKey(), ExprValueUtils.collectionValue(
                    Arrays.stream(es.getValue().fragments()).map(
                        t -> (t.toString())).collect(Collectors.toList())));
              }
              builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));
            }
            return (ExprValue) ExprTupleValue.fromExprValueMap(builder.build());
          }).iterator();
    }
  }
}
