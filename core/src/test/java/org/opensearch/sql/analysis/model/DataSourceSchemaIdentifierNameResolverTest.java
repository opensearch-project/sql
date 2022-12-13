/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.analysis.model;


import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver;

public class DataSourceSchemaIdentifierNameResolverTest {

  @Test
  void testFullyQualifiedName() {
    DataSourceSchemaIdentifierNameResolver
        dataSourceSchemaIdentifierNameResolver = new DataSourceSchemaIdentifierNameResolver(
        Arrays.asList("prom", "information_schema", "tables"), Collections.singleton("prom"));
    Assertions.assertEquals("information_schema",
        dataSourceSchemaIdentifierNameResolver.getSchemaName());
    Assertions.assertEquals("prom", dataSourceSchemaIdentifierNameResolver.getDataSourceName());
    Assertions.assertEquals("tables", dataSourceSchemaIdentifierNameResolver.getIdentifierName());
  }

}
