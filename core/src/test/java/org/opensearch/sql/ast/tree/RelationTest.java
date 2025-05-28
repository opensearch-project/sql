/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.ast.dsl.AstDSL.qualifiedName;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.expression.QualifiedName;

class RelationTest {

  @Test
  void should_return_table_name_if_no_alias() {
    Relation relation = new Relation(qualifiedName("test"));
    assertEquals(
        "test",
        relation.getQualifiedNames().stream()
            .map(QualifiedName::toString)
            .collect(Collectors.joining(",")));
  }

  @Test
  void comma_seperated_index_return_concat_table_names() {
    Relation relation = new Relation(Arrays.asList(qualifiedName("test1"), qualifiedName("test2")));
    assertEquals(
        "test1,test2",
        relation.getQualifiedNames().stream()
            .map(QualifiedName::toString)
            .collect(Collectors.joining(",")));
  }
}
