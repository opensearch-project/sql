/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage.script.sort;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.expression.DSL;

class SortQueryBuilderTest {

  private SortQueryBuilder sortQueryBuilder = new SortQueryBuilder();

  @Test
  void build_sortbuilder_from_reference() {
    assertNotNull(sortQueryBuilder.build(DSL.ref("intV", INTEGER), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void build_sortbuilder_from_function_should_throw_exception() {
    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> sortQueryBuilder.build(DSL.equal(DSL.ref(
            "intV", INTEGER), DSL.literal(1)), Sort.SortOption.DEFAULT_ASC));
    assertThat(exception.getMessage(), Matchers.containsString("unsupported expression"));
  }
}
