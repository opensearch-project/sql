package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.lucene.search.TotalHits;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.request.SqlRequest;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

@RunWith(MockitoJUnitRunner.class)
public class PrettyFormatRestExecutorTest {

  @Mock private SearchResponse searchResponse;
  @Mock private SearchHit searchHit;
  @Mock private DefaultQueryAction queryAction;
  @Mock private SqlRequest sqlRequest;
  private PrettyFormatRestExecutor executor;

  @Before
  public void setUp() {
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    LocalClusterState.state().setPluginSettings(settings);
    when(queryAction.getSqlRequest()).thenReturn(sqlRequest);
    executor = new PrettyFormatRestExecutor("jdbc");
  }

  @Test
  public void testIsDefaultCursor_fetchSizeZero() {
    when(sqlRequest.fetchSize()).thenReturn(0);

    assertFalse(executor.isDefaultCursor(searchResponse, queryAction));
  }

  @Test
  public void testIsDefaultCursor_totalHitsLessThanFetchSize() {
    when(sqlRequest.fetchSize()).thenReturn(10);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(5, TotalHits.Relation.EQUAL_TO), 1.0F));

    assertFalse(executor.isDefaultCursor(searchResponse, queryAction));
  }

  @Test
  public void testIsDefaultCursor_totalHitsGreaterThanOrEqualToFetchSize() {
    when(sqlRequest.fetchSize()).thenReturn(5);
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(5, TotalHits.Relation.EQUAL_TO), 1.0F));

    assertTrue(executor.isDefaultCursor(searchResponse, queryAction));
  }
}
