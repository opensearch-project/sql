/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.storage.Table;

/**
 * Covers the generic {@link OpenSearchCatalogTable} through a {@link SystemIndexCatalogSource}:
 * schema delegation, the predefined-table contract, and the V2 physical path.
 */
@ExtendWith(MockitoExtension.class)
class OpenSearchCatalogTableTest {

  @Mock private OpenSearchClient client;

  @Mock private Table table;

  @Mock private Settings settings;

  private OpenSearchCatalogTable systemTable(String name) {
    return new OpenSearchCatalogTable(new SystemIndexCatalogSource(client, name), settings);
  }

  @Test
  void testGetFieldTypesOfMetaTable() {
    final Map<String, ExprType> fieldTypes = systemTable(TABLE_INFO).getFieldTypes();
    assertThat(fieldTypes, anyOf(hasEntry("TABLE_CAT", STRING)));
  }

  @Test
  void testGetFieldTypesOfMappingTable() {
    final Map<String, ExprType> fieldTypes =
        systemTable(mappingTable("test_index")).getFieldTypes();
    assertThat(fieldTypes, anyOf(hasEntry("COLUMN_NAME", STRING)));
  }

  @Test
  void testIsExist() {
    assertTrue(systemTable(TABLE_INFO).exists());
  }

  @Test
  void testCreateTable() {
    Table systemIndex = systemTable(TABLE_INFO);
    assertThrows(UnsupportedOperationException.class, () -> systemIndex.create(ImmutableMap.of()));
  }

  @Test
  void implement() {
    OpenSearchCatalogTable systemIndex = systemTable(TABLE_INFO);
    NamedExpression projectExpr = named("TABLE_NAME", ref("TABLE_NAME", STRING));

    final PhysicalPlan plan =
        systemIndex.implement(project(relation(TABLE_INFO, table), projectExpr));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(plan.getChild().get(0) instanceof OpenSearchSystemIndexScan);
  }
}
