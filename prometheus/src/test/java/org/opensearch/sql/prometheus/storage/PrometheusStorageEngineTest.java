/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class PrometheusStorageEngineTest {

  @Mock
  private PrometheusClient client;

  @Test
  public void getTable() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Table table = engine.getTable("test");
    assertNull(table);
  }

  @Test
  public void getFunctions() {
    PrometheusStorageEngine engine = new PrometheusStorageEngine(client);
    Collection<FunctionResolver> functionResolverCollection = engine.getFunctions();
    assertNotNull(functionResolverCollection);
    assertEquals(1, functionResolverCollection.size());
    assertTrue(
        functionResolverCollection.iterator().next() instanceof QueryRangeTableFunctionResolver);
  }

}
