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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.executor.pagination.PlanSerializer;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.ContinuePageRequestBuilder;
import org.opensearch.sql.opensearch.request.InitialPageRequestBuilder;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.PagedRequestBuilder;
import org.opensearch.sql.opensearch.request.SerializedPageRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;

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
        new OpenSearchRequest.IndexName("test"), mock(), mock(), exprValueFactory);
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
        new OpenSearchRequest.IndexName("test"), mock(), mock(), exprValueFactory);
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
        new OpenSearchRequest.IndexName("test"), mock(), mock(), exprValueFactory);
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
  @SneakyThrows
  void serialization() {
    PagedRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    OpenSearchResponse response = mock();
    when(request.toCursor()).thenReturn(new SerializedPageRequest("cu-cursor", List.of()));
    when(builder.build()).thenReturn(request);
    var indexName = new OpenSearchRequest.IndexName("index");
    when(builder.getIndexName()).thenReturn(indexName);
    when(client.search(any())).thenReturn(response);
    OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder);
    indexScan.open();

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    ObjectOutputStream objectOutput = new ObjectOutputStream(output);
    objectOutput.writeObject(indexScan);
    objectOutput.flush();

    when(client.getIndexMappings(any())).thenReturn(Map.of());
    OpenSearchStorageEngine engine = mock();
    when(engine.getClient()).thenReturn(client);
    when(engine.getSettings()).thenReturn(mock());
    ObjectInputStream objectInput = new PlanSerializer(engine)
        .getCursorDeserializationStream(new ByteArrayInputStream(output.toByteArray()));
    var roundTripScan = (OpenSearchPagedIndexScan) objectInput.readObject();
    roundTripScan.open();

    // indexScan's request could be a OpenSearchScrollRequest or a ContinuePageRequest, but
    // roundTripScan's request is always a ContinuePageRequest
    // Thus, we can't compare those scans
    //assertEquals(indexScan, roundTripScan);
    // But we can validate that index name and scroll was serialized-deserialized correctly
    assertEquals(indexName, roundTripScan.getRequestBuilder().getIndexName());
    assertTrue(roundTripScan.getRequestBuilder() instanceof ContinuePageRequestBuilder);
    assertEquals(new SerializedPageRequest("cu-cursor", List.of()),
        ((ContinuePageRequestBuilder) roundTripScan.getRequestBuilder())
            .getSerializedPageRequest());
  }

  @Test
  @SneakyThrows
  void dont_serialize_if_no_cursor() {
    PagedRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    OpenSearchResponse response = mock();
    when(builder.build()).thenReturn(request);
    when(client.search(any())).thenReturn(response);
    OpenSearchPagedIndexScan indexScan = new OpenSearchPagedIndexScan(client, builder);
    indexScan.open();

    when(request.toCursor()).thenReturn(null);
    assertThrows(NoCursorException.class, () -> {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(indexScan);
      objectOutput.flush();
    });
  }
}
