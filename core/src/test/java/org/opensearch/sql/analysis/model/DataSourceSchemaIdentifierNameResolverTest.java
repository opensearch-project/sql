/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_SCHEMA_NAME;
import static org.opensearch.sql.analysis.model.DataSourceSchemaIdentifierNameResolverTest.Identifier.identifierOf;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;
import org.opensearch.sql.datasource.DataSourceService;

@ExtendWith(MockitoExtension.class)
public class DataSourceSchemaIdentifierNameResolverTest {

  @Mock
  private DataSourceService dataSourceService;

  @Test
  void testFullyQualifiedName() {
    when(dataSourceService.dataSourceExists("prom")).thenReturn(Boolean.TRUE);
    identifierOf(
            Arrays.asList("prom", "information_schema", "tables"), dataSourceService)
        .datasource("prom")
        .schema("information_schema")
        .name("tables");
  }

  @Test
  void defaultDataSourceNameResolve() {
    when(dataSourceService.dataSourceExists(any())).thenReturn(Boolean.FALSE);
    identifierOf(Arrays.asList("tables"), dataSourceService)
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema(DEFAULT_SCHEMA_NAME)
        .name("tables");

    when(dataSourceService.dataSourceExists(any())).thenReturn(Boolean.FALSE);
    identifierOf(Arrays.asList("information_schema", "tables"), dataSourceService)
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema("information_schema")
        .name("tables");

    when(dataSourceService.dataSourceExists(any())).thenReturn(Boolean.TRUE);
    identifierOf(
            Arrays.asList(DEFAULT_DATASOURCE_NAME, "information_schema", "tables"),
            dataSourceService)
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema("information_schema")
        .name("tables");
  }

  static class Identifier {
    private final DataSourceSchemaIdentifierNameResolver resolver;

    protected static Identifier identifierOf(List<String> parts,
                                             DataSourceService dataSourceService) {
      return new Identifier(parts, dataSourceService);
    }

    Identifier(List<String> parts, DataSourceService dataSourceService) {
      resolver = new DataSourceSchemaIdentifierNameResolver(dataSourceService, parts);
    }

    Identifier datasource(String expectedDatasource) {
      assertEquals(expectedDatasource, resolver.getDataSourceName());
      return this;
    }

    Identifier schema(String expectedSchema) {
      assertEquals(expectedSchema, resolver.getSchemaName());
      return this;
    }

    Identifier name(String expectedName) {
      assertEquals(expectedName, resolver.getIdentifierName());
      return this;
    }
  }
}
