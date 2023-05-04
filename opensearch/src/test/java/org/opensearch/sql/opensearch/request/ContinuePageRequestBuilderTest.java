/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@ExtendWith(MockitoExtension.class)
class ContinuePageRequestBuilderTest {

  @Mock
  private OpenSearchExprValueFactory exprValueFactory;

  private final String scrollId = "scroll";

  private ContinuePageRequestBuilder requestBuilder;

  @BeforeEach
  void setup() {
    var timeout = TimeValue.timeValueMinutes(1);
    requestBuilder = new ContinuePageRequestBuilder(scrollId, timeout, exprValueFactory);
  }

  @Test
  void build() {
    assertEquals(
        new ContinuePageRequest(scrollId, TimeValue.timeValueMinutes(1), exprValueFactory),
        requestBuilder.build(null, 0, null)
    );
  }
}
