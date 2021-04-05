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

package com.amazon.opendistroforelasticsearch.sql.elasticsearch.storage;

import static com.amazon.opendistroforelasticsearch.sql.data.type.ExprCoreType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;

import com.amazon.opendistroforelasticsearch.sql.common.setting.Settings;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValue;
import com.amazon.opendistroforelasticsearch.sql.data.model.ExprValueUtils;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.client.OpenSearchClient;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.data.value.OpenSearchExprValueFactory;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.OpenSearchQueryRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.request.OpenSearchRequest;
import com.amazon.opendistroforelasticsearch.sql.elasticsearch.response.OpenSearchResponse;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;

@ExtendWith(MockitoExtension.class)
class OpenSearchIndexScanTest {

  @Mock
  private OpenSearchClient client;

  @Mock
  private Settings settings;

  private OpenSearchExprValueFactory exprValueFactory = new OpenSearchExprValueFactory(
      ImmutableMap.of("name", STRING, "department", STRING));

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
  }

  @Test
  void queryEmptyResult() {
    mockResponse();
    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "test", exprValueFactory)) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void queryAllResults() {
    mockResponse(
        new ExprValue[]{employee(1, "John", "IT"), employee(2, "Smith", "HR")},
        new ExprValue[]{employee(3, "Allen", "IT")});

    try (OpenSearchIndexScan indexScan =
             new OpenSearchIndexScan(client, settings, "employees", exprValueFactory)) {
      indexScan.open();

      assertTrue(indexScan.hasNext());
      assertEquals(employee(1, "John", "IT"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(2, "Smith", "HR"), indexScan.next());

      assertTrue(indexScan.hasNext());
      assertEquals(employee(3, "Allen", "IT"), indexScan.next());

      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void pushDownFilters() {
    assertThat()
        .pushDown(QueryBuilders.termQuery("name", "John"))
        .shouldQuery(QueryBuilders.termQuery("name", "John"))
        .pushDown(QueryBuilders.termQuery("age", 30))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30)))
        .pushDown(QueryBuilders.rangeQuery("balance").gte(10000))
        .shouldQuery(
            QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("name", "John"))
                .filter(QueryBuilders.termQuery("age", 30))
                .filter(QueryBuilders.rangeQuery("balance").gte(10000)));
  }

  private PushDownAssertion assertThat() {
    return new PushDownAssertion(client, exprValueFactory, settings);
  }

  private static class PushDownAssertion {
    private final OpenSearchClient client;
    private final OpenSearchIndexScan indexScan;
    private final OpenSearchResponse response;
    private final OpenSearchExprValueFactory factory;

    public PushDownAssertion(OpenSearchClient client,
                             OpenSearchExprValueFactory valueFactory,
                             Settings settings) {
      this.client = client;
      this.indexScan = new OpenSearchIndexScan(client, settings, "test", valueFactory);
      this.response = mock(OpenSearchResponse.class);
      this.factory = valueFactory;
      when(response.isEmpty()).thenReturn(true);
    }

    PushDownAssertion pushDown(QueryBuilder query) {
      indexScan.pushDown(query);
      return this;
    }

    PushDownAssertion shouldQuery(QueryBuilder expected) {
      OpenSearchRequest request = new OpenSearchQueryRequest("test", 200, factory);
      request.getSourceBuilder()
             .query(expected)
             .sort(DOC_FIELD_NAME, ASC);
      when(client.search(request)).thenReturn(response);
      indexScan.open();
      return this;
    }
  }

  private void mockResponse(ExprValue[]... searchHitBatches) {
    when(client.search(any()))
        .thenAnswer(
            new Answer<OpenSearchResponse>() {
              private int batchNum;

              @Override
              public OpenSearchResponse answer(InvocationOnMock invocation) {
                OpenSearchResponse response = mock(OpenSearchResponse.class);
                int totalBatch = searchHitBatches.length;
                if (batchNum < totalBatch) {
                  when(response.isEmpty()).thenReturn(false);
                  ExprValue[] searchHit = searchHitBatches[batchNum];
                  when(response.iterator()).thenReturn(Arrays.asList(searchHit).iterator());
                } else if (batchNum == totalBatch) {
                  when(response.isEmpty()).thenReturn(true);
                } else {
                  fail("Search request after empty response returned already");
                }

                batchNum++;
                return response;
              }
            });
  }

  protected ExprValue employee(int docId, String name, String department) {
    SearchHit hit = new SearchHit(docId);
    hit.sourceRef(
        new BytesArray("{\"name\":\"" + name + "\",\"department\":\"" + department + "\"}"));
    return tupleValue(hit);
  }

  private ExprValue tupleValue(SearchHit hit) {
    return ExprValueUtils.tupleValue(hit.getSourceAsMap());
  }
}
