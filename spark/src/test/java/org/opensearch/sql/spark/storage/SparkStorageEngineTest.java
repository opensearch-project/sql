/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.resolver.SparkSqlTableFunctionResolver;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
public class SparkStorageEngineTest {
  @Mock
  private SparkClient client;

  @Test
  public void getFunctions() {
    SparkStorageEngine engine = new SparkStorageEngine(client);
    Collection<FunctionResolver> functionResolverCollection
        = engine.getFunctions();
    assertNotNull(functionResolverCollection);
    assertEquals(1, functionResolverCollection.size());
    assertTrue(
        functionResolverCollection.iterator().next() instanceof SparkSqlTableFunctionResolver);
  }

  @Test
  public void getTable() {
    SparkStorageEngine engine = new SparkStorageEngine(client);
    RuntimeException exception = assertThrows(RuntimeException.class,
        () -> engine.getTable(new DataSourceSchemaName("spark", "default"), ""));
    assertEquals("Unable to get table from storage engine.", exception.getMessage());
  }
}
