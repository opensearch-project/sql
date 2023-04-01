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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.employee;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.mockResponse;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.InitialPageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.PagedRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchPagedIndexScanTest {
  @Mock
  private OpenSearchClient client;

  private final OpenSearchExprValueFactory exprValueFactory = new OpenSearchExprValueFactory(
      ImmutableMap.of(
          "name", OpenSearchDataType.of(STRING),
          "department", OpenSearchDataType.of(STRING)));

  @Test
  void query_empty_result() {
    mockResponse(client);
    InitialPageRequestBuilder builder = new InitialPageRequestBuilder(
        new OpenSearchRequest.IndexName("test"), 3, mock(), exprValueFactory);
    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder)) {
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

    PagedRequestBuilder builder = new InitialPageRequestBuilder(
        new OpenSearchRequest.IndexName("test"), 3, mock(), exprValueFactory);
    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder)) {
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

    builder = new ContinuePageRequestBuilder(
        new OpenSearchRequest.IndexName("test"), "scroll", mock(), exprValueFactory);
    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder)) {
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
        new OpenSearchRequest.IndexName("test"), "scroll", mock(), exprValueFactory);
    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder)) {
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

    builder = new ContinuePageRequestBuilder(
        new OpenSearchRequest.IndexName("test"), "scroll", mock(), exprValueFactory);
    try (OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder)) {
      indexScan.open();

      assertFalse(indexScan.hasNext());
    }
    verify(client, times(2)).cleanup(any());
  }

  @Test
  void explain_not_implemented() {
    assertThrows(Throwable.class, () -> mock(OpenSearchPagedIndexScan.class,
        withSettings().defaultAnswer(CALLS_REAL_METHODS)).explain());
  }

  @Test
  void toCursor() {
    PagedRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    OpenSearchResponse response = mock();
    when(builder.build()).thenReturn(request);
    when(builder.getIndexName()).thenReturn(new OpenSearchRequest.IndexName("index"));
    when(client.search(request)).thenReturn(response);
    when(response.isEmpty()).thenReturn(true);
    when(request.toCursor()).thenReturn("cu-cursor", "", null);
    OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder);
    indexScan.open();
    assertAll(
        () -> assertEquals("(OpenSearchPagedIndexScan,index,cu-cursor)", indexScan.toCursor()),
        () -> assertEquals("", indexScan.toCursor()),
        () -> assertEquals("", indexScan.toCursor())
    );
  }
}
