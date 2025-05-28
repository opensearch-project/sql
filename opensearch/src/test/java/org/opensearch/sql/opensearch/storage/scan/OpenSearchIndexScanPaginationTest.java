/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.QUERY_SIZE;
import static org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexScanTest.mockResponse;

import java.io.ByteArrayOutputStream;
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
import org.opensearch.sql.exception.NoCursorException;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class OpenSearchIndexScanPaginationTest {

  public static final OpenSearchRequest.IndexName INDEX_NAME =
      new OpenSearchRequest.IndexName("test");
  public static final int MAX_RESULT_WINDOW = 3;
  public static final TimeValue SCROLL_TIMEOUT = TimeValue.timeValueMinutes(4);
  @Mock private Settings settings;

  @BeforeEach
  void setup() {
    lenient().when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(QUERY_SIZE);
    lenient()
        .when(settings.getSettingValue(Settings.Key.SQL_CURSOR_KEEP_ALIVE))
        .thenReturn(TimeValue.timeValueMinutes(1));
    lenient().when(settings.getSettingValue(Settings.Key.FIELD_TYPE_TOLERANCE)).thenReturn(true);
  }

  @Mock private OpenSearchClient client;

  private final OpenSearchExprValueFactory exprValueFactory =
      new OpenSearchExprValueFactory(
          Map.of(
              "name", OpenSearchDataType.of(STRING),
              "department", OpenSearchDataType.of(STRING)),
          true);

  @Test
  void query_empty_result() {
    mockResponse(client);
    var builder = new OpenSearchRequestBuilder(QUERY_SIZE, exprValueFactory, settings);
    try (var indexScan =
        new OpenSearchIndexScan(
            client,
            MAX_RESULT_WINDOW,
            builder.build(INDEX_NAME, MAX_RESULT_WINDOW, SCROLL_TIMEOUT, client))) {
      indexScan.open();
      assertFalse(indexScan.hasNext());
    }
    verify(client).cleanup(any());
  }

  @Test
  void explain_not_implemented() {
    assertThrows(
        Throwable.class,
        () ->
            mock(OpenSearchIndexScan.class, withSettings().defaultAnswer(CALLS_REAL_METHODS))
                .explain());
  }

  @Test
  @SneakyThrows
  void dont_serialize_if_no_cursor() {
    OpenSearchRequestBuilder builder = mock();
    OpenSearchRequest request = mock();
    OpenSearchResponse response = mock();
    when(builder.build(any(), anyInt(), any(), any())).thenReturn(request);
    when(client.search(any())).thenReturn(response);
    try (var indexScan =
        new OpenSearchIndexScan(
            client,
            MAX_RESULT_WINDOW,
            builder.build(INDEX_NAME, MAX_RESULT_WINDOW, SCROLL_TIMEOUT, client))) {
      indexScan.open();

      when(request.hasAnotherBatch()).thenReturn(false);
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      assertThrows(NoCursorException.class, () -> objectOutput.writeObject(indexScan));
    }
  }
}
