/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
public class CatalogTableTest {

  @Mock
  private CatalogService catalogService;

  @Test
  void testGetFieldTypes() {
    CatalogTable catalogTable = new CatalogTable(catalogService);
    Map<String, ExprType> fieldTypes =  catalogTable.getFieldTypes();
    Map<String, ExprType> expectedTypes = new HashMap<>();
    expectedTypes.put("CATALOG_NAME", ExprCoreType.STRING);
    expectedTypes.put("CONNECTOR_TYPE", ExprCoreType.STRING);
    assertEquals(expectedTypes, fieldTypes);
  }

  @Test
  void testImplement() {
    CatalogTable catalogTable = new CatalogTable(catalogService);
    PhysicalPlan physicalPlan
        = catalogTable.implement(LogicalPlanDSL.relation(".CATALOGS", catalogTable));
    assertTrue(physicalPlan instanceof CatalogTableScan);
  }

  // todo. temporary added for code coverage. remove if required.
  @Test
  void testExist() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class,
            () -> new CatalogTable(catalogService).exists());
    assertEquals("Unsupported Operation", exception.getMessage());
  }

  // todo. temporary added for code coverage. remove if required.
  @Test
  void testCreateTable() {
    UnsupportedOperationException exception =
        assertThrows(UnsupportedOperationException.class,
            () -> new CatalogTable(catalogService).create(new HashMap<>()));
    assertEquals("Unsupported Operation", exception.getMessage());
  }
}
