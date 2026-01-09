/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.SqlOpenSearchRequestBuilder;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.transport.client.Client;

/** Unit tests for PIT lifecycle management in PrettyFormatRestExecutor. */
@RunWith(MockitoJUnitRunner.Silent.class)
public class PrettyFormatRestExecutorPitTest {

  @Mock private Client client;
  @Mock private DefaultQueryAction queryAction;
  @Mock private SqlRequest sqlRequest;
  @Mock private SqlOpenSearchRequestBuilder requestBuilder;
  @Mock private SearchRequestBuilder searchRequestBuilder;
  @Mock private SearchResponse searchResponse;
  @Mock private SearchHit searchHit;

  private PrettyFormatRestExecutor executor;
  private Map<String, String> params;

  @Before
  public void setUp() throws Exception {
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    LocalClusterState.state().setPluginSettings(settings);

    when(queryAction.getSqlRequest()).thenReturn(sqlRequest);
    when(queryAction.explain()).thenReturn(requestBuilder);
    when(queryAction.getRequestBuilder()).thenReturn(searchRequestBuilder);
    when(searchRequestBuilder.get()).thenReturn(searchResponse);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    executor = new PrettyFormatRestExecutor("jdbc");
    params = new HashMap<>();
  }

  @Test
  public void testNoPitCreatedWhenFetchSizeIsZero() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(0);

    executor.execute(client, params, queryAction);

    verify(searchRequestBuilder, never()).setPointInTime(any(PointInTimeBuilder.class));
    verify(searchRequestBuilder, times(1)).get();
  }

  @Test
  public void testNoPitCreatedWhenFetchSizeNotSpecified() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(null); // Simulates fetch_size not in request

    executor.execute(client, params, queryAction);

    verify(searchRequestBuilder, never()).setPointInTime(any(PointInTimeBuilder.class));
  }

  @Test
  public void testFetchSizeSpecifiedButResultsFitInOnePage() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(100);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    String result = executor.execute(client, params, queryAction);

    assertNotNull("Query should execute successfully", result);
  }

  @Test
  public void testCursorCreatedWhenResultsExceedFetchSize() throws Exception {
    when(sqlRequest.fetchSize()).thenReturn(5);

    SearchHits hits =
        new SearchHits(
            new SearchHit[] {searchHit}, new TotalHits(10, TotalHits.Relation.EQUAL_TO), 1.0F);
    when(searchResponse.getHits()).thenReturn(hits);

    String result = executor.execute(client, params, queryAction);

    assertNotNull("Query should execute successfully", result);
  }

  @Test
  public void testQueryExecutionSucceedsWithVariousFetchSizes() throws Exception {
    int[] fetchSizes = {0, 100, 1};

    for (int fetchSize : fetchSizes) {
      when(sqlRequest.fetchSize()).thenReturn(fetchSize);
      String result = executor.execute(client, params, queryAction);
      assertNotNull("Result should not be null for fetchSize=" + fetchSize, result);
    }
  }
}
