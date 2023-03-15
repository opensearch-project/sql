/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_SCHEMA_NAME;
import static org.opensearch.sql.analysis.model.DataSourceSchemaIdentifierNameResolverTest.Identifier.identifierOf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;

public class DataSourceSchemaIdentifierNameResolverTest {

  @Test
  void testFullyQualifiedName() {
    identifierOf(
            Arrays.asList("prom", "information_schema", "tables"), Collections.singleton("prom"))
        .datasource("prom")
        .schema("information_schema")
        .name("tables");
  }

  @Test
  void defaultDataSourceNameResolve() {
    identifierOf(Arrays.asList("tables"), Collections.emptySet())
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema(DEFAULT_SCHEMA_NAME)
        .name("tables");

    identifierOf(Arrays.asList("information_schema", "tables"), Collections.emptySet())
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema("information_schema")
        .name("tables");

    identifierOf(
            Arrays.asList(DEFAULT_DATASOURCE_NAME, "information_schema", "tables"),
            Collections.emptySet())
        .datasource(DEFAULT_DATASOURCE_NAME)
        .schema("information_schema")
        .name("tables");
  }

  static class Identifier {
    private final DataSourceSchemaIdentifierNameResolver resolver;

    protected static Identifier identifierOf(List<String> parts, Set<String> allowedDataSources) {
      return new Identifier(parts, allowedDataSources);
    }

    Identifier(List<String> parts, Set<String> allowedDataSources) {
      resolver = new DataSourceSchemaIdentifierNameResolver(parts, allowedDataSources);
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
