/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.response;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.sql.data.model.ExprFloatValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

@ExtendWith(MockitoExtension.class)
class OpenSearchResponseTest {

  @Mock private SearchResponse searchResponse;

  @Mock private OpenSearchExprValueFactory factory;

  @Mock private SearchHit searchHit1;

  @Mock private SearchHit searchHit2;

  @Mock private Aggregations aggregations;

  private List<String> includes = List.of();

  @Mock private OpenSearchAggregationResponseParser parser;

  private ExprTupleValue exprTupleValue1 =
      ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1", new ExprIntegerValue(1)));

  private ExprTupleValue exprTupleValue2 =
      ExprTupleValue.fromExprValueMap(ImmutableMap.of("id2", new ExprIntegerValue(2)));

  @Test
  void isEmpty() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1, searchHit2},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    var response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertFalse(response.isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(null);

    response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertTrue(response.isEmpty());

    when(searchResponse.getHits())
        .thenReturn(new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0));
    response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertTrue(response.isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(new Aggregations(emptyList()));

    response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertFalse(response.isEmpty());
  }

  @Test
  void iterator() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1, searchHit2},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    when(searchHit1.getSourceAsString()).thenReturn("{\"id1\", 1}");
    when(searchHit2.getSourceAsString()).thenReturn("{\"id1\", 2}");
    when(factory.construct(any(), anyBoolean()))
        .thenReturn(exprTupleValue1)
        .thenReturn(exprTupleValue2);

    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, List.of("id1"), false)) {
      if (i == 0) {
        assertEquals(exprTupleValue1.tupleValue().get("id"), hit.tupleValue().get("id"));
      } else if (i == 1) {
        assertEquals(exprTupleValue2.tupleValue().get("id"), hit.tupleValue().get("id"));
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void iterator_metafields() {

    ExprTupleValue exprTupleHit =
        ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1", new ExprIntegerValue(1)));

    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                3.75F));

    ShardId shardId = new ShardId("index", "indexUUID", 42);
    SearchShardTarget shardTarget = new SearchShardTarget("node", shardId, null, null);

    when(searchHit1.getSourceAsString()).thenReturn("{\"id1\", 1}");
    when(searchHit1.getId()).thenReturn("testId");
    when(searchHit1.getIndex()).thenReturn("testIndex");
    when(searchHit1.getShard()).thenReturn(shardTarget);
    when(searchHit1.getScore()).thenReturn(3.75F);
    when(searchHit1.getSeqNo()).thenReturn(123456L);

    when(factory.construct(any(), anyBoolean())).thenReturn(exprTupleHit);

    ExprTupleValue exprTupleResponse =
        ExprTupleValue.fromExprValueMap(
            ImmutableMap.of(
                "id1", new ExprIntegerValue(1),
                "_index", new ExprStringValue("testIndex"),
                "_id", new ExprStringValue("testId"),
                "_routing", new ExprStringValue(shardTarget.toString()),
                "_sort", new ExprLongValue(123456L),
                "_score", new ExprFloatValue(3.75F),
                "_maxscore", new ExprFloatValue(3.75F)));
    List includes = List.of("id1", "_index", "_id", "_routing", "_sort", "_score", "_maxscore");
    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, includes, false)) {
      if (i == 0) {
        assertEquals(exprTupleResponse, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void iterator_metafields_withoutIncludes() {

    ExprTupleValue exprTupleHit =
        ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1", new ExprIntegerValue(1)));

    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                3.75F));

    when(searchHit1.getSourceAsString()).thenReturn("{\"id1\", 1}");

    when(factory.construct(any(), anyBoolean())).thenReturn(exprTupleHit);

    List includes = List.of("id1");
    ExprTupleValue exprTupleResponse =
        ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1", new ExprIntegerValue(1)));
    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, includes, true)) {
      if (i == 0) {
        assertEquals(exprTupleResponse, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void iterator_metafields_scoreNaN() {

    ExprTupleValue exprTupleHit =
        ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1", new ExprIntegerValue(1)));

    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                Float.NaN));

    when(searchHit1.getSourceAsString()).thenReturn("{\"id1\", 1}");
    when(searchHit1.getId()).thenReturn("testId");
    when(searchHit1.getIndex()).thenReturn("testIndex");
    when(searchHit1.getScore()).thenReturn(Float.NaN);
    when(searchHit1.getSeqNo()).thenReturn(123456L);

    when(factory.construct(any(), anyBoolean())).thenReturn(exprTupleHit);

    List includes = List.of("id1", "_index", "_id", "_sort", "_score", "_maxscore");
    ExprTupleValue exprTupleResponse =
        ExprTupleValue.fromExprValueMap(
            ImmutableMap.of(
                "id1", new ExprIntegerValue(1),
                "_index", new ExprStringValue("testIndex"),
                "_id", new ExprStringValue("testId"),
                "_sort", new ExprLongValue(123456L)));
    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, includes, true)) {
      if (i == 0) {
        assertEquals(exprTupleResponse, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void iterator_with_inner_hits() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    when(factory.construct(any(), anyBoolean())).thenReturn(exprTupleValue1);

    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, includes, false)) {
      assertEquals(exprTupleValue1, hit);
    }
  }

  @Test
  void response_is_aggregation_when_aggregation_not_empty() {
    when(searchResponse.getAggregations()).thenReturn(aggregations);

    OpenSearchResponse response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertTrue(response.isAggregationResponse());
  }

  @Test
  void response_isnot_aggregation_when_aggregation_is_empty() {
    when(searchResponse.getAggregations()).thenReturn(null);

    OpenSearchResponse response = new OpenSearchResponse(searchResponse, factory, includes, false);
    assertFalse(response.isAggregationResponse());
  }

  @Test
  void aggregation_iterator() {
    final List includes = List.of("id1", "id2");

    when(parser.parse(any()))
        .thenReturn(Arrays.asList(ImmutableMap.of("id1", 1), ImmutableMap.of("id2", 2)));
    when(searchResponse.getAggregations()).thenReturn(aggregations);
    when(factory.getParser()).thenReturn(parser);
    when(factory.construct(anyString(), anyInt(), anyBoolean()))
        .thenReturn(new ExprIntegerValue(1))
        .thenReturn(new ExprIntegerValue(2));

    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory, includes, false)) {
      if (i == 0) {
        assertEquals(exprTupleValue1, hit);
      } else if (i == 1) {
        assertEquals(exprTupleValue2, hit);
      } else {
        fail("More search hits returned than expected");
      }
      i++;
    }
  }

  @Test
  void highlight_iterator() {
    SearchHit searchHit = new SearchHit(1);
    searchHit.sourceRef(new BytesArray("{\"name\":\"John\"}"));
    Map<String, HighlightField> highlightMap =
        Map.of("highlights", new HighlightField("Title", new Text[] {new Text("field")}));
    searchHit.highlightFields(
        Map.of("highlights", new HighlightField("Title", new Text[] {new Text("field")})));
    ExprValue resultTuple = ExprValueUtils.tupleValue(searchHit.getSourceAsMap());

    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1},
                new TotalHits(1L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    when(searchHit1.getHighlightFields()).thenReturn(highlightMap);
    when(factory.construct(any(), anyBoolean())).thenReturn(resultTuple);

    for (ExprValue resultHit : new OpenSearchResponse(searchResponse, factory, includes, false)) {
      var expected =
          ExprValueUtils.collectionValue(
              Arrays.stream(searchHit.getHighlightFields().get("highlights").getFragments())
                  .map(t -> (t.toString()))
                  .collect(Collectors.toList()));
      var result = resultHit.tupleValue().get("_highlight").tupleValue().get("highlights");
      assertTrue(expected.equals(result));
    }
  }
}
