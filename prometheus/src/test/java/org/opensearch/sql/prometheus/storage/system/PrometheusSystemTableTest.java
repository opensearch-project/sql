/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.system;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.expression.DSL.named;
import static org.opensearch.sql.expression.DSL.ref;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.project;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
public class PrometheusSystemTableTest {

  @Mock
  private PrometheusClient client;

  @Mock
  private Table table;

  @Test
  void testGetFieldTypesOfMetaTable() {
    PrometheusSystemTable systemIndex = new PrometheusSystemTable(client,
        new DataSourceSchemaName("prometheus", "information_schema"), TABLE_INFO);
    final Map<String, ExprType> fieldTypes = systemIndex.getFieldTypes();
    assertThat(fieldTypes, anyOf(
        hasEntry("TABLE_CATALOG", STRING)
    ));
    assertThat(fieldTypes, anyOf(
        hasEntry("UNIT", STRING)
    ));
  }

  @Test
  void testGetFieldTypesOfMappingTable() {
    PrometheusSystemTable systemIndex = new PrometheusSystemTable(client,
        new DataSourceSchemaName("prometheus", "information_schema"), mappingTable(
        "test_metric"));
    final Map<String, ExprType> fieldTypes = systemIndex.getFieldTypes();
    assertThat(fieldTypes, anyOf(
        hasEntry("COLUMN_NAME", STRING)
    ));
  }



  @Test
  void implement() {
    PrometheusSystemTable systemIndex = new PrometheusSystemTable(client,
        new DataSourceSchemaName("prometheus", "information_schema"), TABLE_INFO);
    NamedExpression projectExpr = named("TABLE_NAME", ref("TABLE_NAME", STRING));

    final PhysicalPlan plan = systemIndex.implement(
        project(
            relation(TABLE_INFO, table),
            projectExpr
        ));
    assertTrue(plan instanceof ProjectOperator);
    assertTrue(plan.getChild().get(0) instanceof PrometheusSystemTableScan);
  }

}
