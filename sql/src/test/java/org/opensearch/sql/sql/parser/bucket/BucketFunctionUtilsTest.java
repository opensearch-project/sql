/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql.parser.bucket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class BucketFunctionUtilsTest {

  @Test
  void normalizeFieldRef_string_literal_becomes_qualified_name() {
    UnresolvedExpression result =
        BucketFunctionUtils.normalizeFieldRef(AstDSL.stringLiteral("age"));
    assertEquals(AstDSL.qualifiedName("age"), result);
  }

  @Test
  void normalizeFieldRef_qualified_name_passes_through_unchanged() {
    QualifiedName input = AstDSL.qualifiedName("age");
    assertSame(input, BucketFunctionUtils.normalizeFieldRef(input));
  }

  @Test
  void applyMissing_null_returns_field_unchanged() {
    QualifiedName field = AstDSL.qualifiedName("age");
    assertSame(field, BucketFunctionUtils.applyMissing(field, null));
  }

  @Test
  void applyMissing_non_null_wraps_with_coalesce() {
    QualifiedName field = AstDSL.qualifiedName("age");
    UnresolvedExpression missing = AstDSL.intLiteral(0);
    assertEquals(
        new Function("coalesce", List.of(field, missing)),
        BucketFunctionUtils.applyMissing(field, missing));
  }
}
