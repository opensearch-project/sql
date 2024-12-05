/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;

@ExtendWith(MockitoExtension.class)
class OpenSearchCatIndicesRequestTest {

  @Mock private OpenSearchClient client;

  @Test
  void testSearch() {
    when(client.indices()).thenReturn(List.of("index"));

    final List<ExprValue> results = new OpenSearchCatIndicesRequest(client).search();
    assertEquals(1, results.size());
    assertThat(results.get(0).tupleValue(), anyOf(hasEntry("TABLE_NAME", stringValue("index"))));
  }

  @Test
  void testToString() {
    assertEquals(
        "OpenSearchCatIndicesRequest{}", new OpenSearchCatIndicesRequest(client).toString());
  }
}
