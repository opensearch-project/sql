/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.planner.logical.LogicalPlanDSL.relation;
import static org.opensearch.sql.utils.SystemIndexUtils.mappingTable;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.mapping.IndexMapping;
import org.opensearch.sql.opensearch.storage.OpenSearchStorageEngine;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * DESCRIBE names a knn_vector field {@code vector} only in PPL with the Calcite engine enabled. PPL
 * with Calcite off reports {@code array} and SQL reports {@code knn_vector}, as before.
 */
class SystemIndexCatalogSourceVectorTest {

  private final OpenSearchClient client = mock(OpenSearchClient.class);

  {
    IndexMapping mapping = mock(IndexMapping.class);
    when(mapping.getFieldMappings())
        .thenReturn(
            OpenSearchDataType.parseMapping(
                Map.of("embedding", Map.of("type", "knn_vector", "dimension", 3))));
    when(client.getIndexMappings("vec")).thenReturn(Map.of("vec", mapping));
  }

  private static ExprValue typeName(PhysicalPlan scan) {
    scan.open();
    return scan.next().tupleValue().get("TYPE_NAME");
  }

  private ExprValue typeName(String table, boolean calciteEngine) {
    return typeName(
        new OpenSearchCatalogTable(
                new SystemIndexCatalogSource(client, table, calciteEngine), mock(Settings.class))
            .implement(relation(table, mock(Table.class))));
  }

  @Test
  void pplDescribeSaysVectorOnlyWithCalcite() {
    assertEquals(stringValue("vector"), typeName(mappingTable("vec", PPL_SPEC), true));
    assertEquals(stringValue("array"), typeName(mappingTable("vec", PPL_SPEC), false));
  }

  @Test
  void sqlDescribeKeepsKnnVector() {
    assertEquals(stringValue("knn_vector"), typeName(mappingTable("vec"), true));
    assertEquals(stringValue("knn_vector"), typeName(mappingTable("vec"), false));
  }

  @Test
  void storageEngineReadsTheCalciteSetting() {
    for (boolean calcite : new boolean[] {true, false}) {
      Settings settings = mock(Settings.class);
      when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(calcite);
      String name = mappingTable("vec", PPL_SPEC);
      Table table =
          new OpenSearchStorageEngine(client, settings)
              .getTable(new DataSourceSchemaName(DEFAULT_DATASOURCE_NAME, "default"), name);
      assertEquals(
          stringValue(calcite ? "vector" : "array"),
          typeName(((OpenSearchCatalogTable) table).implement(relation(name, table))));
    }
  }
}
