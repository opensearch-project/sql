/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.catalog;

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
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.catalog.model.Catalog;
import org.opensearch.sql.catalog.model.ConnectorType;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.storage.StorageEngine;

@ExtendWith(MockitoExtension.class)
public class CatalogTableScanTest {

  @Mock
  private CatalogService catalogService;

  @Mock
  private StorageEngine storageEngine;

  private CatalogTableScan catalogTableScan;

  @BeforeEach
  private void setUp() {
    catalogTableScan = new CatalogTableScan(catalogService);
  }

  @Test
  void testExplain() {
    assertEquals("GetCatalogRequestRequest{}", catalogTableScan.explain());
  }

  @Test
  void testIterator() {
    Set<Catalog> catalogSet = new HashSet<>();
    catalogSet.add(new Catalog("prometheus", ConnectorType.PROMETHEUS, storageEngine));
    catalogSet.add(new Catalog("opensearch", ConnectorType.OPENSEARCH, storageEngine));
    when(catalogService.getCatalogs()).thenReturn(catalogSet);

    assertFalse(catalogTableScan.hasNext());
    catalogTableScan.open();
    assertTrue(catalogTableScan.hasNext());
    for (Catalog catalog : catalogSet) {
      assertEquals(new ExprTupleValue(new LinkedHashMap<>(ImmutableMap.of(
              "DATASOURCE_NAME", ExprValueUtils.stringValue(catalog.getName()),
              "CONNECTOR_TYPE", ExprValueUtils.stringValue(catalog.getConnectorType().name())))),
          catalogTableScan.next());
    }
  }

}
