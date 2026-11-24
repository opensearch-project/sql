/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.response.agg.NoBucketAggregationParser;
import org.opensearch.sql.opensearch.response.agg.SingleValueParser;

class OpenSearchQueryRequestReducedResponseTest {

  private static final TotalHits HITS = new TotalHits(10, TotalHits.Relation.EQUAL_TO);

  @Test
  void aReduceGivesTheRowsOfTheSameResponse() {
    OpenSearchExprValueFactory factory = new OpenSearchExprValueFactory(Map.of(), true);
    factory.setParser(new NoBucketAggregationParser(new SingleValueParser("m")));
    OpenSearchQueryRequest request =
        OpenSearchQueryRequest.of(
            "test",
            new SearchSourceBuilder().size(0).aggregation(AggregationBuilders.max("m").field("n")),
            factory,
            List.of());
    InternalAggregations aggregations =
        InternalAggregations.from(List.of(new InternalMax("m", 7, DocValueFormat.RAW, null)));
    SearchResponse response = mock(SearchResponse.class);
    when(response.getHits()).thenReturn(new SearchHits(new SearchHit[0], HITS, Float.NaN));
    when(response.getAggregations()).thenReturn(aggregations);

    assertEquals(
        rows(new OpenSearchResponse(response, factory, List.of(), false)),
        rows(request.reducedResponse(HITS, aggregations).orElseThrow()));
  }

  private static List<ExprValue> rows(OpenSearchResponse response) {
    return ImmutableList.copyOf(response.iterator());
  }
}
