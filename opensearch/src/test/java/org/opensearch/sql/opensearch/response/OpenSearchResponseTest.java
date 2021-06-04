/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
 */

package org.opensearch.sql.opensearch.response;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;

@ExtendWith(MockitoExtension.class)
class OpenSearchResponseTest {

  @Mock
  private SearchResponse searchResponse;

  @Mock
  private OpenSearchExprValueFactory factory;

  @Mock
  private SearchHit searchHit1;

  @Mock
  private SearchHit searchHit2;

  @Mock
  private Aggregations aggregations;

  @Mock
  private OpenSearchAggregationResponseParser parser;

  private ExprTupleValue exprTupleValue1 = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id1",
      new ExprIntegerValue(1)));

  private ExprTupleValue exprTupleValue2 = ExprTupleValue.fromExprValueMap(ImmutableMap.of("id2",
      new ExprIntegerValue(2)));

  @Test
  void isEmpty() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit1, searchHit2},
                new TotalHits(2L, TotalHits.Relation.EQUAL_TO),
                1.0F));

    assertFalse(new OpenSearchResponse(searchResponse, factory).isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(null);
    assertTrue(new OpenSearchResponse(searchResponse, factory).isEmpty());

    when(searchResponse.getHits())
        .thenReturn(new SearchHits(null, new TotalHits(0, TotalHits.Relation.EQUAL_TO), 0));
    OpenSearchResponse response3 = new OpenSearchResponse(searchResponse, factory);
    assertTrue(response3.isEmpty());

    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(searchResponse.getAggregations()).thenReturn(new Aggregations(emptyList()));
    assertFalse(new OpenSearchResponse(searchResponse, factory).isEmpty());
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
    when(factory.construct(any())).thenReturn(exprTupleValue1).thenReturn(exprTupleValue2);

    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory)) {
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
  void response_is_aggregation_when_aggregation_not_empty() {
    when(searchResponse.getAggregations()).thenReturn(aggregations);

    OpenSearchResponse response = new OpenSearchResponse(searchResponse, factory);
    assertTrue(response.isAggregationResponse());
  }

  @Test
  void response_isnot_aggregation_when_aggregation_is_empty() {
    when(searchResponse.getAggregations()).thenReturn(null);

    OpenSearchResponse response = new OpenSearchResponse(searchResponse, factory);
    assertFalse(response.isAggregationResponse());
  }

  @Test
  void aggregation_iterator() {
    when(parser.parse(any()))
        .thenReturn(Arrays.asList(ImmutableMap.of("id1", 1), ImmutableMap.of("id2", 2)));
    when(searchResponse.getAggregations()).thenReturn(aggregations);
    when(factory.getParser()).thenReturn(parser);
    when(factory.construct(anyString(), any()))
        .thenReturn(new ExprIntegerValue(1))
        .thenReturn(new ExprIntegerValue(2));

    int i = 0;
    for (ExprValue hit : new OpenSearchResponse(searchResponse, factory)) {
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
}
