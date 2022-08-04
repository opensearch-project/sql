/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OpenSearchRequestBuilderTest {

  public static final TimeValue DEFAULT_QUERY_TIMEOUT = TimeValue.timeValueMinutes(1L);
  @Mock
  private Settings settings;

  @Mock
  private OpenSearchExprValueFactory factory;

  @BeforeEach
  void setup() {
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(200);
  }

  @Test
  void buildQueryRequest() {
    Integer maxResultWindow = 500;
    Integer limit = 200;
    Integer offset = 0;
    OpenSearchRequestBuilder builder =
        new OpenSearchRequestBuilder("test", maxResultWindow, settings, factory);
    builder.pushDownLimit(limit, offset);

    assertEquals(
        new OpenSearchQueryRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(limit)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            factory),
        builder.build());
  }

  @Test
  void buildScrollRequestWithCorrectSize() {
    Integer maxResultWindow = 500;
    Integer limit = 800;
    Integer offset = 10;
    OpenSearchRequestBuilder builder =
        new OpenSearchRequestBuilder("test", maxResultWindow, settings, factory);
    builder.pushDownLimit(limit, offset);

    assertEquals(
        new OpenSearchScrollRequest(
            new OpenSearchRequest.IndexName("test"),
            new SearchSourceBuilder()
                .from(offset)
                .size(maxResultWindow - offset)
                .timeout(DEFAULT_QUERY_TIMEOUT),
            factory),
        builder.build());
  }
}
