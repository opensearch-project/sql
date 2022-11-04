/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.datasource.DatasourceService;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
public class DatasourceTableTest {

  @Mock
  private DatasourceService datasourceService;

  @Test
  void testGetFieldTypes() {
    DatasourceTable datasourceTable = new DatasourceTable(datasourceService);
    Map<String, ExprType> fieldTypes =  datasourceTable.getFieldTypes();
    Map<String, ExprType> expectedTypes = new HashMap<>();
    expectedTypes.put("CATALOG_NAME", ExprCoreType.STRING);
    expectedTypes.put("CONNECTOR_TYPE", ExprCoreType.STRING);
    assertEquals(expectedTypes, fieldTypes);
  }

  @Test
  void testImplement() {
    DatasourceTable datasourceTable = new DatasourceTable(datasourceService);
    PhysicalPlan physicalPlan
        = datasourceTable.implement(LogicalPlanDSL.relation(".CATALOGS", datasourceTable));
    assertTrue(physicalPlan instanceof DatasourcesTableScan);
  }

}
