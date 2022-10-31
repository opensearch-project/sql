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
import org.opensearch.sql.analysis.CatalogSchemaIdentifierNameResolver;

public class CatalogSchemaIdentifierNameResolverTest {

  @Test
  void testFullyQualifiedName() {
    CatalogSchemaIdentifierNameResolver
        catalogSchemaIdentifierNameResolver = new CatalogSchemaIdentifierNameResolver(
        Arrays.asList("prom", "information_schema", "tables"), Collections.singleton("prom"));
    Assertions.assertEquals("information_schema",
        catalogSchemaIdentifierNameResolver.getSchemaName());
    Assertions.assertEquals("prom", catalogSchemaIdentifierNameResolver.getCatalogName());
    Assertions.assertEquals("tables", catalogSchemaIdentifierNameResolver.getIdentifierName());
  }

}
