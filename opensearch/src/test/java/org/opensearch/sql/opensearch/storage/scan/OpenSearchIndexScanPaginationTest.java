/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.QUERY_SIZE;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.employee;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.mockResponse;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.mockTwoPageResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.ExecutableRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchIndexScanPaginationTest {

  public static final OpenSearchRequest.IndexName INDEX_NAME
      = new OpenSearchRequest.IndexName("test");
  public static final int MAX_RESULT_WINDOW = 3;
  public static final String SCROLL_ID = "0xbadbeef";
  public static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueMinutes(4);
  @Mock
  private Settings settings;

  @BeforeEach
  void setup() {
    lenient().when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(QUERY_SIZE);
    lenient().when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
      .thenReturn(TimeValue.timeValueMinutes(1));
  }

  @Mock
  private OpenSearchClient client;

  private final OpenSearchExprValueFactory exprValueFactory
      = new OpenSearchExprValueFactory(Map.of(
      "name", OpenSearchDataType.of(STRING),
      "department", OpenSearchDataType.of(STRING)));

  @Test
  void query_empty_result() {
    mockResponse(client);
    var builder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (var indexScan
           = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void query_all_results_initial_scroll_request() {
    mockResponse(client, new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT")});

    ExecutableRequestBuilder builder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory);
    try (var indexScan
           = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
      );
    }
    verify(client).cleanup(any());

    builder = new ContinuePageRequestBuilder(SCROLL_ID, SCROLL_TIMEOUT, exprValueFactory);
    try (var indexScan
           = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();

      assertFalse(indexScan.hasNext());
    }
    verify(client, times(2)).cleanup(any());
  }

  @Test
  void query_all_results_continuation_scroll_request() {
    mockResponse(client, new ExprValue[]{
        employee(1, "John", "IT"),
        employee(2, "Smith", "HR"),
        employee(3, "Allen", "IT")});

    ContinuePageRequestBuilder builder = new ContinuePageRequestBuilder(
        SCROLL_ID, SCROLL_TIMEOUT, exprValueFactory);
    try (var indexScan
           = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();

      assertAll(
          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(1, "John", "IT"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(2, "Smith", "HR"), indexScan.next()),

          () -> assertTrue(indexScan.hasNext()),
          () -> assertEquals(employee(3, "Allen", "IT"), indexScan.next()),

          () -> assertFalse(indexScan.hasNext()),
          () -> assertEquals(3, indexScan.getTotalHits())
      );
    }
    verify(client).cleanup(any());

    builder = new ContinuePageRequestBuilder(SCROLL_ID, SCROLL_TIMEOUT, exprValueFactory);
    try (var indexScan
           = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();

      assertFalse(indexScan.hasNext());
    }
    verify(client, times(2)).cleanup(any());
  }

  @Test
  void explain_not_implemented() {
    assertThrows(Throwable.class, () -> mock(OpenSearchIndexScan.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS)).explain());
  }

  @Test
  @SneakyThrows
  void serialization() {
    mockTwoPageResponse(client);
    OpenSearchRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    when(request.toCursor()).thenReturn("cu-cursor");
    when(builder.build(any(), eq(MAX_RESULT_WINDOW), any())).thenReturn(request);
    var indexScan = new OpenSearchIndexScan(client, INDEX_NAME, settings,
        MAX_RESULT_WINDOW, builder);
    indexScan.open();

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(indexScan);
    objectOutput.flush();

    when(client.getIndexMappings(any())).thenReturn(Map.of());
    OpenSearchStorageEngine engine = mock();
    when(engine.getClient()).thenReturn(client);
    when(engine.getSettings()).thenReturn(mock());
    when(client.getIndexMaxResultWindows(any()))
        .thenReturn((Map.of(INDEX_NAME.getIndexNames()[0], MAX_RESULT_WINDOW)));
    ObjectInputStream objectInput = new PlanSerializer(engine)
        .getCursorDeserializationStream(new ByteArrayInputStream(output.toByteArray()));
    var roundTripScan = (OpenSearchIndexScan) objectInput.readObject();
    roundTripScan.open();
    assertTrue(roundTripScan.hasNext());
  }

  @Test
  @SneakyThrows
  void dont_serialize_if_no_cursor() {
    OpenSearchRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    OpenSearchResponse response = mock();
    when(builder.build(any(), anyInt(), any())).thenReturn(request);
    when(client.search(any())).thenReturn(response);
    try (var indexScan
        = new OpenSearchIndexScan(client, INDEX_NAME, settings, MAX_RESULT_WINDOW, builder)) {
      indexScan.open();

      when(request.toCursor()).thenReturn(null, "");
      for (int i = 0; i < 2; i++) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ObjectOutputStream objectOutput = new ObjectOutputStream(output);
        assertThrows(NoCursorException.class, () -> objectOutput.writeObject(indexScan));
      }
    }
  }
}
