/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.prometheus.request.system.PrometheusSystemRequest;

@ExtendWith(MockitoExtension.class)
public class PrometheusSystemTableScanTest {

  @Mock private PrometheusSystemRequest request;

  @Test
  public void queryData() {
    when(request.search()).thenReturn(Collections.singletonList(stringValue("text")));
    final PrometheusSystemTableScan systemIndexScan = new PrometheusSystemTableScan(request);

    systemIndexScan.open();
    assertTrue(systemIndexScan.hasNext());
    assertEquals(stringValue("text"), systemIndexScan.next());
  }

  @Test
  public void explain() {
    when(request.toString()).thenReturn("request");
    final PrometheusSystemTableScan systemIndexScan = new PrometheusSystemTableScan(request);
    assertEquals("request", systemIndexScan.explain());
  }
}
