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
import org.opensearch.sql.analysis.DatasourceSchemaIdentifierNameResolver;

public class DatasourceSchemaIdentifierNameResolverTest {

  @Test
  void testFullyQualifiedName() {
    DatasourceSchemaIdentifierNameResolver
        datasourceSchemaIdentifierNameResolver = new DatasourceSchemaIdentifierNameResolver(
        Arrays.asList("prom", "information_schema", "tables"), Collections.singleton("prom"));
    Assertions.assertEquals("information_schema",
        datasourceSchemaIdentifierNameResolver.getSchemaName());
    Assertions.assertEquals("prom", datasourceSchemaIdentifierNameResolver.getDatasourceName());
    Assertions.assertEquals("tables", datasourceSchemaIdentifierNameResolver.getIdentifierName());
  }

}
