/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.cursor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.cursor.CursorType;
import org.opensearch.sql.legacy.cursor.DefaultCursor;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;

public class DefaultCursorTest {
  @Mock private OpenSearchSettings settings;

  @Mock private SearchSourceBuilder sourceBuilder;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    // Required for Pagination queries using PIT instead of Scroll
    doReturn(Collections.emptyList()).when(settings).getSettings();
    when(settings.getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER)).thenReturn(true);
    LocalClusterState.state().setPluginSettings(settings);

    // Mock the toXContent method of SearchSourceBuilder
    try {
      XContentBuilder xContentBuilder = XContentFactory.jsonBuilder(new ByteArrayOutputStream());
      when(sourceBuilder.toXContent(any(XContentBuilder.class), any())).thenReturn(xContentBuilder);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void checkCursorType() {
    DefaultCursor cursor = new DefaultCursor();
    assertEquals(cursor.getType(), CursorType.DEFAULT);
  }

  @Test
  public void cursorShouldStartWithCursorTypeIDForPIT() {
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setPitId("dbdskbcdjksbcjkdsbcjk+//");
    cursor.setIndexPattern("myIndex");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());

    // Set the mocked SearchSourceBuilder to the cursor
    cursor.setSearchSourceBuilder(sourceBuilder);

    assertThat(cursor.generateCursorId(), startsWith(cursor.getType().getId() + ":"));
  }

  @Test
  public void cursorShouldStartWithCursorTypeIDForScroll() {
    // Disable PIT for pagination and use scroll instead
    when(settings.getSettingValue(Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER)).thenReturn(false);

    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setScrollId("dbdskbcdjksbcjkdsbcjk+//");
    cursor.setIndexPattern("myIndex");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());

    // Set the mocked SearchSourceBuilder to the cursor
    cursor.setSearchSourceBuilder(sourceBuilder);

    assertThat(cursor.generateCursorId(), startsWith(cursor.getType().getId() + ":"));
  }

  @Test
  public void nullCursorWhenRowLeftIsLessThanEqualZero() {
    DefaultCursor cursor = new DefaultCursor();
    assertThat(cursor.generateCursorId(), emptyOrNullString());

    cursor.setRowsLeft(-10);
    assertThat(cursor.generateCursorId(), emptyOrNullString());
  }

  @Test
  public void nullCursorWhenScrollIDIsNullOrEmpty() {
    DefaultCursor cursor = new DefaultCursor();
    assertThat(cursor.generateCursorId(), emptyOrNullString());

    cursor.setScrollId("");
    assertThat(cursor.generateCursorId(), emptyOrNullString());
  }
}
