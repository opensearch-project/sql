/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

/**
 * Unit tests for shouldCreateCursor() method.
 *
 * <p>For PIT lifecycle tests, see {@link PrettyFormatRestExecutorPitTest}.
 */
@RunWith(MockitoJUnitRunner.class)
public class PrettyFormatRestExecutorTest {

  @Mock private SearchResponse searchResponse;
  @Mock private SearchHit searchHit;
  @Mock private DefaultQueryAction queryAction;
  private PrettyFormatRestExecutor executor;

  @Before
  public void setUp() {
    OpenSearchSettings settings = mock(OpenSearchSettings.class);
    LocalClusterState.state().setPluginSettings(settings);
    executor = new PrettyFormatRestExecutor("jdbc");
  }

  @Test
  public void testShouldCreateCursor_fetchSizeZero() {
    assertFalse(executor.shouldCreateCursor(searchResponse, queryAction, 0));
  }

  @Test
  public void testShouldCreateCursor_totalHitsLessThanFetchSize() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(5, TotalHits.Relation.EQUAL_TO), 1.0F));

    assertFalse(executor.shouldCreateCursor(searchResponse, queryAction, 10));
  }

  @Test
  public void testShouldCreateCursor_totalHitsGreaterThanOrEqualToFetchSize() {
    when(searchResponse.getHits())
        .thenReturn(
            new SearchHits(
                new SearchHit[] {searchHit}, new TotalHits(5, TotalHits.Relation.EQUAL_TO), 1.0F));

    assertTrue(executor.shouldCreateCursor(searchResponse, queryAction, 5));
  }
}
