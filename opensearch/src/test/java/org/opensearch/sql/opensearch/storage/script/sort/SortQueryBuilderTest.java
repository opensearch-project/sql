/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.script.sort;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.data.model.ExprShortValue;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.LiteralExpression;
import org.opensearch.sql.opensearch.expression.OpenSearchDSL;

class SortQueryBuilderTest {

  private final SortQueryBuilder sortQueryBuilder = new SortQueryBuilder();

  @Test
  void build_sortbuilder_from_reference() {
    assertNotNull(sortQueryBuilder.build(DSL.ref("intV", INTEGER), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void build_sortbuilder_from_nested_function() {
    assertNotNull(
        sortQueryBuilder.build(
            OpenSearchDSL.nested(DSL.ref("message.info", STRING)), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void build_sortbuilder_from_nested_function_with_path_param() {
    assertNotNull(
        sortQueryBuilder.build(
            OpenSearchDSL.nested(DSL.ref("message.info", STRING), DSL.ref("message", STRING)),
            Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void nested_with_too_many_args_throws_exception() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sortQueryBuilder.build(
                OpenSearchDSL.nested(
                    DSL.ref("message.info", STRING),
                    DSL.ref("message", STRING),
                    DSL.ref("message", STRING)),
                Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void nested_with_too_few_args_throws_exception() {
    assertThrows(
        IllegalArgumentException.class,
        () -> sortQueryBuilder.build(OpenSearchDSL.nested(), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void nested_with_invalid_arg_type_throws_exception() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sortQueryBuilder.build(
                OpenSearchDSL.nested(DSL.literal(1)), Sort.SortOption.DEFAULT_ASC));
  }

  @Test
  void build_sortbuilder_from_expression_should_throw_exception() {
    final IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                sortQueryBuilder.build(
                    new LiteralExpression(new ExprShortValue(1)), Sort.SortOption.DEFAULT_ASC));
    assertThat(exception.getMessage(), Matchers.containsString("unsupported expression"));
  }

  @Test
  void build_sortbuilder_from_function_should_throw_exception() {
    final IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                sortQueryBuilder.build(
                    DSL.equal(DSL.ref("intV", INTEGER), DSL.literal(1)),
                    Sort.SortOption.DEFAULT_ASC));
    assertThat(exception.getMessage(), Matchers.containsString("unsupported expression"));
  }
}
