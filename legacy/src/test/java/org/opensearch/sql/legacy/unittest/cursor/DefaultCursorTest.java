/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.unittest.cursor;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
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

  @Test
  public void indicesAreSerializedIntoCursorId() {
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setPitId("pit-id");
    cursor.setIndexPattern("idx1|idx2");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());
    cursor.setIndices(new String[] {"idx1", "idx2"});
    cursor.setSearchSourceBuilder(sourceBuilder);

    String cursorId = cursor.generateCursorId();
    JSONObject decoded = decodePayload(cursorId);

    assertEquals(2, decoded.getJSONArray("x").length());
    assertEquals("idx1", decoded.getJSONArray("x").getString(0));
    assertEquals("idx2", decoded.getJSONArray("x").getString(1));
  }

  @Test
  public void nullIndicesAreSerializedAsEmptyArray() {
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setPitId("pit-id");
    cursor.setIndexPattern("idx1");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());
    cursor.setSearchSourceBuilder(sourceBuilder);

    String cursorId = cursor.generateCursorId();
    JSONObject decoded = decodePayload(cursorId);

    assertEquals(0, decoded.getJSONArray("x").length());
  }

  @Test
  public void deserializeRoundTripsIndices() {
    SearchSourceBuilder realSource = new SearchSourceBuilder();
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setPitId("pit-id");
    cursor.setIndexPattern("idx1|idx2");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());
    cursor.setIndices(new String[] {"idx1", "idx2"});
    cursor.setSearchSourceBuilder(realSource);

    String cursorId = cursor.generateCursorId();
    String payload = cursorId.substring(cursorId.indexOf(':') + 1);
    DefaultCursor restored = DefaultCursor.from(payload);

    assertArrayEquals(new String[] {"idx1", "idx2"}, restored.getIndices());
  }

  @Test
  public void deserializeLegacyCursorWithoutIndicesDefaultsToEmptyArray() {
    // Legacy cursor payloads written before this fix do not contain the "x" field.
    // They must continue to deserialize cleanly with indices == [] so in-flight
    // cursors from pre-fix nodes are not rejected after upgrade.
    SearchSourceBuilder realSource = new SearchSourceBuilder();
    DefaultCursor cursor = new DefaultCursor();
    cursor.setRowsLeft(50);
    cursor.setPitId("pit-id");
    cursor.setIndexPattern("idx1");
    cursor.setFetchSize(500);
    cursor.setFieldAliasMap(Collections.emptyMap());
    cursor.setColumns(new ArrayList<>());
    cursor.setIndices(new String[] {"idx1"});
    cursor.setSearchSourceBuilder(realSource);

    String cursorId = cursor.generateCursorId();
    String payload = cursorId.substring(cursorId.indexOf(':') + 1);
    JSONObject json = new JSONObject(new String(Base64.getDecoder().decode(payload)));
    json.remove("x");
    String legacyPayload = Base64.getEncoder().encodeToString(json.toString().getBytes());

    DefaultCursor restored = DefaultCursor.from(legacyPayload);

    assertArrayEquals(new String[0], restored.getIndices());
  }

  private JSONObject decodePayload(String cursorId) {
    String payload = cursorId.substring(cursorId.indexOf(':') + 1);
    return new JSONObject(new String(Base64.getDecoder().decode(payload)));
  }
}
