/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.physical.datasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.SystemIndexUtils.DATASOURCES_TABLE_NAME;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
public class DataSourceTableTest {

  @Mock private DataSourceService dataSourceService;

  @Test
  void testGetFieldTypes() {
    DataSourceTable dataSourceTable = new DataSourceTable(dataSourceService);
    Map<String, ExprType> fieldTypes = dataSourceTable.getFieldTypes();
    Map<String, ExprType> expectedTypes = new HashMap<>();
    expectedTypes.put("DATASOURCE_NAME", ExprCoreType.STRING);
    expectedTypes.put("CONNECTOR_TYPE", ExprCoreType.STRING);
    assertEquals(expectedTypes, fieldTypes);
  }

  @Test
  void testImplement() {
    DataSourceTable dataSourceTable = new DataSourceTable(dataSourceService);
    PhysicalPlan physicalPlan =
        dataSourceTable.implement(LogicalPlanDSL.relation(DATASOURCES_TABLE_NAME, dataSourceTable));
    assertTrue(physicalPlan instanceof DataSourceTableScan);
  }

  // todo. temporary added for code coverage. remove if required.
  @Test
  void testExist() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> new DataSourceTable(dataSourceService).exists());
    assertEquals("Unsupported Operation", exception.getMessage());
  }

  // todo. temporary added for code coverage. remove if required.
  @Test
  void testCreateTable() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> new DataSourceTable(dataSourceService).create(new HashMap<>()));
    assertEquals("Unsupported Operation", exception.getMessage());
  }

  @Test
  void defaultAsStreamingSource() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> new DataSourceTable(dataSourceService).asStreamingSource());
  }
}
