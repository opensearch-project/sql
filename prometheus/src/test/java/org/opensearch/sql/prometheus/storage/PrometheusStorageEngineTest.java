/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.utils.SystemIndexUtils.TABLE_INFO;

import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.prometheus.storage.system.PrometheusSystemTable;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class PrometheusStorageEngineTest {

  @Mock
  private PrometheusClient client;

  @Test
  public void getTable() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Table table = engine.getTable(new DataSourceSchemaName("prometheus", "default"), "test", null);
    assertNotNull(table);
    assertTrue(table instanceof PrometheusMetricTable);
  }

  @Test
  public void getFunctions() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Collection<FunctionResolver> functionResolverCollection
        = engine.getFunctions();
    assertNotNull(functionResolverCollection);
    assertEquals(1, functionResolverCollection.size());
    assertTrue(
        functionResolverCollection.iterator().next() instanceof QueryRangeTableFunctionResolver);
  }

  @Test
  public void getSystemTable() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Table table = engine.getTable(new DataSourceSchemaName("prometheus", "default"), TABLE_INFO, "ignored");
    assertNotNull(table);
    assertTrue(table instanceof PrometheusSystemTable);
  }

  @Test
  public void getSystemTableForAllTablesInfo() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Table table
        = engine.getTable(new DataSourceSchemaName("prometheus", "information_schema"), "tables", "ignored");
    assertNotNull(table);
    assertTrue(table instanceof PrometheusSystemTable);
  }

  @Test
  public void getSystemTableWithWrongInformationSchemaTable() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    SemanticCheckException exception = assertThrows(SemanticCheckException.class,
        () -> engine.getTable(new DataSourceSchemaName("prometheus", "information_schema"),
            "test", "ignored"));
    assertEquals("Information Schema doesn't contain test table", exception.getMessage());
  }

}
