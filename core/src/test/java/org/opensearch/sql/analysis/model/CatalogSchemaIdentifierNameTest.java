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

public class CatalogSchemaIdentifierNameTest {

  @Test
  void testFullyQualifiedName() {
    CatalogSchemaIdentifierName catalogSchemaIdentifierName = new CatalogSchemaIdentifierName(
        Arrays.asList("prom", "information_schema", "tables"), Collections.singleton("prom"));
    Assertions.assertEquals("information_schema", catalogSchemaIdentifierName.getSchemaName());
    Assertions.assertEquals("prom", catalogSchemaIdentifierName.getCatalogName());
    Assertions.assertEquals("tables", catalogSchemaIdentifierName.getIdentifierName());
  }

}
