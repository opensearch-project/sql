/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATAFIELD_TYPE_MAP;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ID;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_INDEX;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_MAXSCORE;
import static org.opensearch.sql.opensearch.storage.OpenSearchIndex.METADATA_FIELD_ROUTING;
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
import org.opensearch.core.common.text.Text;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

/** OpenSearch search response. */
@EqualsAndHashCode
@ToString
public class OpenSearchResponse implements Iterable<ExprValue> {

  /** Search query result (non-aggregation). */
  private final SearchHits hits;

  /** Search aggregation result. */
  private final Aggregations aggregations;

  /** List of requested include fields. */
  private final List<String> includes;

  /** OpenSearchExprValueFactory used to build ExprValue from search result. */
  @EqualsAndHashCode.Exclude private final OpenSearchExprValueFactory exprValueFactory;

  /** Constructor of OpenSearchResponse. */
  public OpenSearchResponse(
      SearchResponse searchResponse,
      OpenSearchExprValueFactory exprValueFactory,
      List<String> includes) {
    this.hits = searchResponse.getHits();
    this.aggregations = searchResponse.getAggregations();
    this.exprValueFactory = exprValueFactory;
    this.includes = includes;
  }

  /** Constructor of OpenSearchResponse with SearchHits. */
  public OpenSearchResponse(
      SearchHits hits, OpenSearchExprValueFactory exprValueFactory, List<String> includes) {
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
      return handleAggregationResponse();
    } else {
      return Arrays.stream(hits.getHits())
          .map(
              hit -> {
                ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
                addParsedHitsToBuilder(builder, hit);
                addMetaDataFieldsToBuilder(builder, hit);
                addHighlightsToBuilder(builder, hit);
                return (ExprValue) ExprTupleValue.fromExprValueMap(builder.build());
              })
          .iterator();
    }
  }

  /**
   * Parse response for all hits to add to builder. Inner_hits supports arrays of objects with
   * nested type.
   *
   * @param builder builder to build values from response.
   * @param hit Search hit from response.
   */
  private void addParsedHitsToBuilder(
      ImmutableMap.Builder<String, ExprValue> builder, SearchHit hit) {
    builder.putAll(
        exprValueFactory
            .construct(
                hit.getSourceAsString(),
                !(hit.getInnerHits() == null || hit.getInnerHits().isEmpty()))
            .tupleValue());
  }

  /**
   * If highlight fields are present in response add the fields to the builder.
   *
   * @param builder builder to build values from response.
   * @param hit Search hit from response.
   */
  private void addHighlightsToBuilder(
      ImmutableMap.Builder<String, ExprValue> builder, SearchHit hit) {
    if (!hit.getHighlightFields().isEmpty()) {
      var hlBuilder = ImmutableMap.<String, ExprValue>builder();
      for (var es : hit.getHighlightFields().entrySet()) {
        hlBuilder.put(
            es.getKey(),
            ExprValueUtils.collectionValue(
                Arrays.stream(es.getValue().fragments())
                    .map(Text::toString)
                    .collect(Collectors.toList())));
      }
      builder.put("_highlight", ExprTupleValue.fromExprValueMap(hlBuilder.build()));
    }
  }

  /**
   * Add metadata fields to builder from response.
   *
   * @param builder builder to build values from response.
   * @param hit Search hit from response.
   */
  private void addMetaDataFieldsToBuilder(
      ImmutableMap.Builder<String, ExprValue> builder, SearchHit hit) {
    List<String> metaDataFieldSet =
        includes.isEmpty()
            ? METADATAFIELD_TYPE_MAP.keySet().stream().toList()
            : includes.stream().filter(METADATAFIELD_TYPE_MAP::containsKey).toList();
    ExprFloatValue maxScore =
        Float.isNaN(hits.getMaxScore()) ? null : new ExprFloatValue(hits.getMaxScore());

    metaDataFieldSet.forEach(
        metaDataField -> {
          if (metaDataField.equals(METADATA_FIELD_INDEX)) {
            builder.put(METADATA_FIELD_INDEX, new ExprStringValue(hit.getIndex()));
          } else if (metaDataField.equals(METADATA_FIELD_ID)) {
            builder.put(METADATA_FIELD_ID, new ExprStringValue(hit.getId()));
          } else if (metaDataField.equals(METADATA_FIELD_SCORE)) {
            if (!Float.isNaN(hit.getScore())) {
              builder.put(METADATA_FIELD_SCORE, new ExprFloatValue(hit.getScore()));
            }
          } else if (metaDataField.equals(METADATA_FIELD_MAXSCORE)) {
            if (maxScore != null) {
              builder.put(METADATA_FIELD_MAXSCORE, maxScore);
            }
          } else if (metaDataField.equals(METADATA_FIELD_SORT)) {
            builder.put(METADATA_FIELD_SORT, new ExprLongValue(hit.getSeqNo()));
          } else { // if (metaDataField.equals(METADATA_FIELD_ROUTING)){
            builder.put(
                METADATA_FIELD_ROUTING,
                new ExprStringValue(hit.getShard() == null ? null : hit.getShard().toString()));
          }
        });
  }

  /**
   * Handle an aggregation response.
   *
   * @return Parsed and built return values from response.
   */
  private Iterator<ExprValue> handleAggregationResponse() {
    return exprValueFactory.getParser().parse(aggregations).stream()
        .map(
            entry -> {
              ImmutableMap.Builder<String, ExprValue> builder = new ImmutableMap.Builder<>();
              for (Map.Entry<String, Object> value : entry.entrySet()) {
                builder.put(
                    value.getKey(),
                    exprValueFactory.construct(value.getKey(), value.getValue(), false));
              }
              return (ExprValue) ExprTupleValue.fromExprValueMap(builder.build());
            })
        .iterator();
  }
}
