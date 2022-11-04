/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.DatasourceService;
import org.opensearch.sql.datasource.model.Datasource;
import org.opensearch.sql.datasource.model.ConnectorType;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
public class DatasourceTableScanTest {

  @Mock
  private DatasourceService datasourceService;

  @Mock
  private StorageEngine storageEngine;

  private DatasourcesTableScan datasourcesTableScan;

  @BeforeEach
  private void setUp() {
    datasourcesTableScan = new DatasourcesTableScan(datasourceService);
  }

  @Test
  void testExplain() {
    assertEquals("GetCatalogRequestRequest{}", datasourcesTableScan.explain());
  }

  @Test
  void testIterator() {
    Set<Datasource> datasourceSet = new HashSet<>();
    datasourceSet.add(new Datasource("prometheus", ConnectorType.PROMETHEUS, storageEngine));
    datasourceSet.add(new Datasource("opensearch", ConnectorType.OPENSEARCH, storageEngine));
    when(datasourceService.getDatasources()).thenReturn(datasourceSet);

    assertFalse(datasourcesTableScan.hasNext());
    datasourcesTableScan.open();
    assertTrue(datasourcesTableScan.hasNext());
    for (Datasource datasource : datasourceSet) {
      assertEquals(new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
              "CATALOG_NAME", ExprValueUtils.stringValue(datasource.getName()),
              "CONNECTOR_TYPE", ExprValueUtils.stringValue(datasource.getConnectorType().name())))),
          datasourcesTableScan.next());
    }
  }

}
