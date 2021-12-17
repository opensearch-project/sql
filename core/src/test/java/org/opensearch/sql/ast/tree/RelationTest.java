/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;

import org.junit.jupiter.api.Test;

class RelationTest {

  @Test
  void should_return_table_name_if_no_alias() {
    Relation relation = new Relation(qualifiedName("test"));
    assertEquals("test", relation.getTableName());
    assertEquals("test", relation.getTableNameOrAlias());
  }

  @Test
  void should_return_alias_if_aliased() {
    Relation relation = new Relation(qualifiedName("test"), "t");
    assertEquals("t", relation.getTableNameOrAlias());
  }

}
