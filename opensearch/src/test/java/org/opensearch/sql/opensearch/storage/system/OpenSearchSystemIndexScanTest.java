/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.opensearch.request.system.OpenSearchSystemRequest;

@ExtendWith(MockitoExtension.class)
class OpenSearchSystemIndexScanTest {

  @Mock private OpenSearchSystemRequest request;

  @Test
  public void queryData() {
    when(request.search()).thenReturn(singletonList(stringValue("text")));
    final OpenSearchSystemIndexScan systemIndexScan = new OpenSearchSystemIndexScan(request);

    systemIndexScan.open();
    assertTrue(systemIndexScan.hasNext());
    assertEquals(stringValue("text"), systemIndexScan.next());
  }

  @Test
  public void explain() {
    when(request.toString()).thenReturn("request");
    final OpenSearchSystemIndexScan systemIndexScan = new OpenSearchSystemIndexScan(request);

    assertEquals("request", systemIndexScan.explain());
  }
}
